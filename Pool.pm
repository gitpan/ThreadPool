package Thread::Pool;
use Thread qw(cond_signal cond_wait);
use fields qw(min max num jobs);
use strict;

use vars '$VERSION';

$VERSION = 0.1;

=head1 NAME

Thread::Pool - worker thread pools to run Perl code asynchronously

=head1 SYNOPSIS

    use Thread::Pool;
    my $pool = Thread::Pool->new(Min => 5, Max => 10);
    $pool->enqueue(\&foo, @args);
    $pool->set_min(7);
    $pool->set_max(20);

=head1 DESCRIPTION

The C<Thread::Pool> module implements pools of worker threads. Once a
thread pool is created, jobs (i.e. Perl subs with arguments) may be
enqueued for the pool to execute. When a thread in the pool becomes
free, it takes another job from the pool's work queue and calls it.
If a job is enqueued while no thread is free and the number of active
threads does not exceed a given maximum, a new thread is added to the
pool immediately to hande the new job. A pool can be configured to
have a minimum number of threads which are always running and waiting
for jobs to carry out. A pool can be configured to have a maximum
number of threads - only that number of threads will ever be created
in the pool and new jobs have to wait until an existing worker thread
becomes free.

=head1 METHODS

=over 8

=item new

Create a new thread pool. Arguments are optional and are of the form
of key/value pairs. Passing C<Min =E<gt> $min> invokes C<set_min($min)>
(q.v.) for the pool. The default min value is zero. Passing
C<Max =E<gt> $max> invokes C<set_max($max)> (q.v.) for the pool. The
default max value is -1.

=item enqueue(CODE [, ARG, ...])

Enqueues a new job for the pool to carry out. CODE can be a reference
to a subroutine or the name of one. Unqualified subroutine names
default to the caller's package. The subroutine will be called
(optionally with any given arguments) in one of the worker threads in
the pool either immediately (if the number of active threads in the
pool has not reached its configured maximum) or when a thread becomes
free (if it has reached its maximum).

=item set_min(MIN)

Sets the minimum number of threads in the pool to MIN. If MIN is
increased then more threads are immediately created in the pool to
bring the number up to MIN. When the queue of jobs for the pool is
emptied, MIN worker threads will wait around for more work rather
than finishing.

=item set_max(MAX)

Sets the maximum number of threads in the pool to MAX meaning that no
more than MAX threads will ever be running concurrently for that pool.
If MAX is -1 then the limit is infinite. That means that if a new job
is enqueued while all threads are active, a new thread is always
created immediately to handle the job.

=back

=head1 SEE ALSO

L<Thread>

=head1 AUTHOR

Malcolm Beattie, mbeattie@sable.ox.ac.uk.

=cut

sub _work {
    my $pool = shift;
    while (defined(my $job = $pool->_next_job)) {
	my $code = shift @$job;
	eval { &$code(@$job) };
    }
    lock $pool;
    $pool->{num}--;
}

sub _next_job {
    use attrs qw(locked method);
    my $pool = shift;
    cond_wait($pool) while $pool->{num} <= $pool->{min} && !@{$pool->{jobs}};
    my $job = $pool->{jobs}->[0];
    shift @{$pool->{jobs}} if defined $job;
    return $job;
}

sub _start_worker {
    use attrs qw(locked method);
    my $pool = shift;
    $pool->{num}++;
    Thread->new(\&_work, $pool)->detach;
}

sub enqueue {
    use attrs qw(locked method);
    my ($pool, $code, @args) = @_;
    if (!ref($code) && $code !~ /::/ && $code !~ /'/) {
	$code = caller() . "::$code";
    }
    push @{$pool->{jobs}}, [$code, @args];
    if ($pool->{max} == -1 || $pool->{num} < $pool->{max}) {
	$pool->_start_worker;
    }
    cond_signal($pool);
}

sub set_min {
    use attrs qw(locked method);
    my ($pool, $min) = @_;
    $pool->{min} = $min;
    for (my $i = $pool->{num}; $i < $min; $i++) {
	$pool->_start_worker;
    }
}

sub set_max {
    use attrs qw(locked method);
    my ($pool, $max) = @_;
    $pool->{max} = $max;
}

sub new {
    my $class = shift;
    my $pool = bless { min => 0, max => -1, num => 0, jobs => [] }, $class;
    while (my ($key, $value) = splice(@_, 0, 2)) {
	if ($key eq "Min") {
	    $pool->set_min($value);
	} elsif ($key eq "Max") {
	    $pool->set_max($value);
	} else {
	    require Carp;
	    Carp::croak("Thread::Pool::new passed bad attribute name '$key'");
	}
    }
    return $pool;
}

1;
