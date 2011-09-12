package Google::Fusion::Result;
use 5.006;
use Moose;
use Carp;

=head1 NAME

Google::Fusion::Result - A Query result

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Google::Fusion::Result;

    my $result = Google::Fusion::Result->new();
    ...

=head1 PARAMS/ACCESSORS

=head2 response

The HTTP::Response object associated with the query

=head2 query

The query string

=head2 result

ArrayRef of the results

=head2 headers

Arrayref of the headers

=head2 columns

Number of columns this result has

=head2 rows

Number of rows this result has (excluding headers)

=cut

has 'query'         => ( is => 'ro', isa => 'Str',                required => 1                       );
has 'response'      => ( is => 'ro', isa => 'HTTP::Response',     required => 1                       );
has 'num_columns'   => ( is => 'rw', isa => 'Int',                required => 1, default => 0         );
has 'num_rows'      => ( is => 'rw', isa => 'Int',                required => 1, default => 0         );
has 'query_time'    => ( is => 'rw', isa => 'Num',                required => 1, default => 0         );
has 'auth_time'     => ( is => 'rw', isa => 'Num',                required => 1, default => 0         );
has 'total_time'    => ( is => 'rw', isa => 'Num',                required => 1, default => 0         );
has 'rows'          => ( 
    is          => 'rw',
    isa         => 'ArrayRef[ArrayRef]',
    required    => 1,
    default     => sub{ [] },
    trigger     => sub{ $_[0]->num_rows( scalar( @{ $_[1] } ) ) },
    );

has 'columns'       => ( 
    is          => 'rw',
    isa         => 'ArrayRef',           
    required    => 1, 
    default     => sub{ [] },
    trigger     => sub{ $_[0]->num_columns( scalar( @{ $_[1] } ) ) },
    );

=head1 SUBROUTINES/METHODS

=cut

=head1 AUTHOR

Robin Clarke, C<< <perl at robinclarke.net> >>

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Robin Clarke.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of Google::Fusion
