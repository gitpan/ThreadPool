use Thread;
use Thread::Pool;

sub say {
    for (my $i = 0; $i < 5; $i++) {
        print "@_\n";
	sleep 1;
    }
}

my $pool = Thread::Pool->new(Max => 2);
for ($i = 0; $i < 5; $i++) {
    $pool->enqueue(\&say, $i);
}
