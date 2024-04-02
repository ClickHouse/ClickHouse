#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;

my @current_stack = ();
my $grouped_stacks = {};

while (my $line = <>)
{
    chomp $line;

    if ($line =~ '^#')
    {
        $line =~ s/^#\d+\s+//;
        $line =~ s/ \([^\)]+=.+\) at / at /g;
        push @current_stack, $line;
    }

    if ($line eq '')
    {
        my $group = \$grouped_stacks;
        for my $frame (reverse @current_stack)
        {
            $$group->{count} ||= 0;
            ++$$group->{count};
            $group = \$$group->{children}{$frame};
        }

        @current_stack = ();
    }
}

sub print_group
{
    my $group = shift;
    my $level = shift || 0;

    for my $key (sort { $group->{children}{$b}{count} <=> $group->{children}{$a}{count} } keys %{$group->{children}})
    {
        my $count = $group->{count};
        print(('| ' x $level) . $count . (' ' x (5 - (length $count))) . $key . "\n");
        print_group($group->{children}{$key}, $level + 1);
    }
}

print_group($grouped_stacks);
