package AnyEvent::RabbitMQ::RPC;

use strict;
use warnings;

use AnyEvent::RabbitMQ;

sub new {
    my $class = shift;
    my %args = @_;

    my $self = bless {}, $class;

    my $cv = AE::cv;

    my $amqp = $args{connection};
    my $channel = sub {
        $amqp->open_channel(
            on_success => sub {
                $self->{channel} = shift;
                $self->{channel}->qos;
                $cv->send($self);
            },
            on_failure => sub {
                warn "Channel failed: @_";
                $cv->send();
            }
        );
    };
    if ($amqp) {
        $channel->();
    } else {
        AnyEvent::RabbitMQ->load_xml_spec;
        $amqp = AnyEvent::RabbitMQ->new(timeout => 1, verbose => 0);
        $amqp->connect(
            %args,
            on_success => $channel,
            on_failure => sub {
                warn "Connect failed: @_";
                $cv->send();
            }
        );
    }

    $args{serialize} ||= '';
    if ($args{serialize} eq "YAML") {
        require YAML::Any;
        $self->{serialize}   = \&YAML::Any::Dump;
        $self->{unserialize} = \&YAML::Any::Load;
    } elsif ($args{serialize} eq "JSON") {
        require JSON::Any;
        JSON::Any->import;
        my $json = JSON::Any->new;
        $self->{serialize}   = sub { $json->objToJson( [@_] ) };
        $self->{unserialize} = sub { (@{ $json->jsonToObj(@_) })[0] };
    } elsif ($args{serialize} eq "Storable") {
        require Storable;
        $self->{serialize}   = sub { Storable::nfreeze( [@_] )};
        $self->{unserialize} = sub { (@{ Storable::thaw(@_) })[0] };
    }

    # Block on having set up the channel
    return $cv->recv;
}

sub channel {
    my $self = shift;
    return $self->{channel};
}

sub rpc_queue {
    my $self = shift;
    my %args = @_;

    # These queues are durable -- as such, we should only need to check
    # that they are there once per process.
    return $args{on_success}->()
        if $self->{queues}{$args{queue}};

    $self->channel->declare_queue(
        no_ack     => 0,
        durable    => 1,
        exclusive  => 0,
        %args,
        on_success => sub {
            $self->{queues}{$args{queue}}++;
            $args{on_success}->();
        },
    );
}

sub reply_queue {
    my $self = shift;
    my %args = @_;

    $self->channel->declare_queue(
        no_ack     => 1,
        durable    => 0,
        exclusive  => 1,
        on_success => sub {
            $args{on_success}->(shift->method_frame->queue);
        },
        on_failure => $args{on_failure},
    );
}

sub register {
    my $self = shift;
    my %args = (
        name => undef,
        run  => sub {},
        on_failure => sub { warn "Failure: @_" },
        @_
    );

    # Ensure we have the queue
    $self->rpc_queue(
        queue      => $args{name},
        on_success => sub {
            # And set up a listen on it
            $self->channel->consume(
                queue      => $args{name},
                no_ack     => 0,
                on_consume => sub {
                    my $frame = shift;
                    my $args = $frame->{body}->payload;
                    $args = $self->{unserialize}->($args)
                        if $self->{unserialize};

                    # Call the sub
                    my $return = $args{run}->( $args );

                    # Send the response, if they asked for it
                    if (my $reply_to = $frame->{header}->reply_to) {
                        $return = $self->{serialize}->($return)
                            if $self->{serialize};
                        $return = "0E0" if not $return;
                        $self->channel->publish(
                            exchange => '',
                            routing_key => $reply_to,
                            body => $return,
                        );
                    }

                    # And finally mark the task as complete
                    $self->channel->ack;
                },
                on_failure => $args{on_failure},
            );
        },
        on_failure => $args{on_failure},
    );
}

sub call {
    my $self = shift;

    my %args = (
        name => undef,
        args => undef,
        on_sent => undef,
        on_failure => sub { warn "Failure: @_" },
        @_
    );

    my $finished;
    if (defined wantarray and not $args{on_reply}) {
        # We we're called in a not-void context, and without a reply
        # callback, assume this is a syncronous call, and set up
        # $finished to block on the reply
        $args{on_reply} = $finished = AE::cv;
        my $fail = $args{on_failure};
        $args{on_failure} = sub {
            $fail->(@_) if $fail;
            $finished->send(undef);
        }
    }

    my $sent_failure = $args{on_sent} ? sub {
        $args{on_sent}->send(0);
        $args{on_failure}->(@_);
    } : $args{on_failure};

    my $send; $send = sub {
        my $REPLIES = shift;
        my $args = $args{args};
        $args = $self->{serialize}->($args)
            if $self->{serialize};
        $args = "0E0" if not $args;
        $self->channel->publish(
            exchange    => '',
            routing_key => $args{name},
            body        => $args,
            header => {
                ($REPLIES ? (reply_to => $REPLIES) : ()),
                delivery_mode => 2, # Persistent storage
            },
        );
        $args{on_sent}->send(1) if $args{on_sent};
    };

    unless ($args{on_reply}) {
        # Fire and forget
        $self->rpc_queue(
            queue      => $args{name},
            on_success => sub { $send->(undef) },
            on_failure => $sent_failure,
        );
        return;
    }

    # We need to set up an ephemeral reply queue
    $self->rpc_queue(
        queue      => $args{name},
        on_success => sub {
            $self->reply_queue(
                on_success => sub {
                    my $REPLIES = shift;
                    $self->channel->consume(
                        queue => $REPLIES,
                        no_ack => 1,
                        on_consume => sub {
                            my $frame = shift;
                            # We got a reply, tear down our reply queue
                            $self->channel->delete_queue(
                                queue => $REPLIES,
                            );
                            my $return = $frame->{body}->payload;
                            $return = $self->{unserialize}->($return)
                                if $self->{unserialize};
                            $args{on_reply}->($return);
                        },
                        on_success => sub { $send->($REPLIES) },
                        on_failure => $sent_failure,
                    );
                },
                on_failure => $sent_failure,
            );
        },
        on_failure => $sent_failure,
    );

    return $finished->recv if $finished;
    return 1;
}

1;
