package Google::Fusion;
use 5.006;

use Moose;
use LWP::UserAgent;
use HTTP::Request;
use URL::Encode qw/url_encode/;
use YAML qw/LoadFile DumpFile Dump/;
use Carp;
use Net::OAuth2::Moosey::Client; 
use Google::Fusion::Result;
use Text::CSV;
use Time::HiRes qw/time/;
use Try::Tiny;
use Digest::SHA qw/sha256_hex/;
use File::Spec::Functions;
use IO::String;

=head1 NAME

Google::Fusion - Interface to the Google Fusion Tables API

=head1 VERSION

Version 0.03

=cut

our $VERSION = '0.03';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Google::Fusion;

    my $foo = Google::Fusion->new();
    ...

=head1 PARAMS/ACCESSORS

One of the following combination of parameters is required:

    client_id and client_secret

You will be prompted with a URL, with which you will atain an access_code.

    client_id, client_secret, access_code

The OAuth2 client will complete the authorization process for you and get the refresh_token and access_token for you

    refresh_token and optionally access_token

The OAuth2 client will get a valid access_token for you if necessary, and refresh it when necessary.

    access_token

You will be able to make requests as long as the access_token is valid.

=head2 client_id

The client id of your application.

=head2 client_secret

The secret for your application

=head2 refresh_token

Refresh token, aquired during the authorization process

=head2 access_token

A temporary access token aquired during the authorization process

=head2 keep_alive

Use keep_alive for connections - this will make the application /much/ more responsive.
Default: 1

=head2 headers

Responses passed with headers.
Default: 1

=head access_code

The code returned during the OAuth2 authorization process with which access_token and refresh_token are aquired.

=head auth_client

A Net::OAuth2::Moosey::Client object with which authenticated requests are made.  If you are running 
in application mode (interactive), then you can accept the default.
If you already have an authenticated client, then initialise with it.
If you have some required parameters (access_token, refresh_token or access_code), but no client
object yet, then just define these parameters, and allow the client to be created for you.

=cut

has 'client_id'     => ( is => 'ro', isa => 'Str',                                  );
has 'client_secret' => ( is => 'ro', isa => 'Str',                                  );
has 'refresh_token' => ( is => 'ro', isa => 'Str',                                  );
has 'access_token'  => ( is => 'ro', isa => 'Str',                                  );
has 'access_code'   => ( is => 'ro', isa => 'Str',                                  );
has 'query_cache'   => ( is => 'ro', isa => 'Str',                                  );
has 'token_store'   => ( is => 'ro', isa => 'Str',                                  );
has 'headers'       => ( is => 'ro', isa => 'Bool', required => 1, default => 1,    );
has 'keep_alive'    => ( is => 'ro', isa => 'Bool', required => 1, default => 1,    );
has 'auth_client'   => ( is => 'ro',                required => 1, lazy => 1,
    isa         => 'Net::OAuth2::Moosey::Client',
    builder     => '_build_auth_client',
    );

# Local method to build the auth_client if it wasn't passed
sub _build_auth_client {
    my $self = shift;

    my %client_params = (
        site_url_base           => 'https://accounts.google.com/o/oauth2/auth',
        access_token_url_base   => 'https://accounts.google.com/o/oauth2/token',
        authorize_url_base      => 'https://accounts.google.com/o/oauth2/auth',
        scope                   => 'https://www.google.com/fusiontables/api/query',        
    );
    foreach( qw/client_id client_secret refresh_token access_code access_token keep_alive token_store/ ){
        $client_params{$_} = $self->$_ if defined $self->$_;
    }
    
    # $self->logger->debug( "Initialising Client with:\n".  Dump( \%client_params ) );
    my $client = Net::OAuth2::Moosey::Client->new( %client_params );
    return $client;
}

=head1 SUBROUTINES/METHODS

=head2 query

Submit a (Googley) SQL query.  Single argument is the SQL.
Return value is a C<Google::Fusion::Result> object

Example:

    my $text = $fusion->query( 'SELECT * FROM 123456' );

=cut
sub get_fresh_access_token {
    my $self    = shift;
    $self->auth_client->get_fresh_access_token();
}

sub query {
    my $self    = shift;
    my $sql     = shift;

    if( $sql !~ m/^(show|describe|create|select|insert|update|delete|drop)/i ){
        die( "That doesn't look like a valid (Fusion) SQL statement...\n" );
    }
   
    # Get a valid access_token before timing the query time
    my $auth_time_start = time();
    $self->auth_client->access_token_object->valid_access_token();
    my $auth_time = time() - $auth_time_start;

    my $query_start = time();
    
    my $response = $self->query_or_cache( $sql );

    my $query_time = time() - $query_start;
    my $result = Google::Fusion::Result->new(
        query       => $sql,
        response    => $response,
        query_time  => $query_time,
        auth_time   => $auth_time,
        total_time  => $query_time + $auth_time,
        );

    if( not $response->is_success ){
        $result->error( sprintf "%s (%u)", $response->message, $response->code );
    }else{
        # Response was a success
        # TODO: RCL 2011-09-08 Parse the actual error message from the response
        # TODO: RCL 2011-09-08 Refresh access_key if it was invalid, or move that
        # action to the Client?

        my $data = $response->decoded_content();
        # print $data; 
        my $csv = Text::CSV->new ( { 
            binary      => 1,  # Reliable handling of UTF8 characters
            escape_char => '"',
            quote_char  => '"',
            } ) or croak( "Cannot use CSV: ".Text::CSV->error_diag () );
        my $io = IO::String->new( $data );
        my $parsed_data = $csv->getline_all( $io );
        $csv->eof or $csv->error_diag();


        # Find the max length of each column
        # TODO: RCL 2011-09-09 This won't handle elements with newlines gracefully...
        my @max;
        foreach my $row_idx( 0 .. scalar( @{ $parsed_data } ) - 1 ){
            foreach my $col_idx ( 0 .. scalar( @{ $parsed_data->[0] } ) - 1 ){
                if( ( not $max[$col_idx] ) or ( length( $parsed_data->[$row_idx][$col_idx] ) > $max[$col_idx] ) ){
                    $max[$col_idx] = length( $parsed_data->[$row_idx][$col_idx] );
                }
            }
        }


        if( $self->headers ){
            $result->columns( shift( @{ $parsed_data } ) );
        }
        $result->rows( $parsed_data );
        $result->has_headers( $self->headers );
        if( not $result->num_columns ){
            $result->num_columns( scalar( @{ $parsed_data->[0] } ) );
        }
        $result->max_lengths( \@max );
        $result->has_headers( $self->headers );
    }
    return $result;
}

sub query_or_cache {
    my $self = shift;
    my $sql = shift;
    my $digest = sha256_hex( $sql );
    # printf "Digest: %s\n", $digest;
    my $cache_file = catfile( $self->query_cache, $digest );
    
    my $response = undef;
    if( $self->query_cache ){
        if( -f $cache_file ){
            $response = LoadFile( $cache_file );
        }
    }
    if( not $response ){
        my @post_args =  ( 'https://www.google.com/fusiontables/api/query',
            HTTP::Headers->new( Content_Type => 'application/x-www-form-urlencoded' ),
            sprintf( 'sql=%s&hdrs=%s',
                url_encode( $sql ),
                ( $self->headers ? 'true' : 'false' ),
                ),
            );


        $response = $self->auth_client->post( @post_args );
        # If the response was not Unauthorized, most likely is that the token is invalid
        # Invalidate the current token, and try again
        if( $response->code == 401 and $response->message eq 'Unauthorized' ){
            # Make the token expire, so a new one is requested
            $self->auth_client->get_fresh_access_token();
            $response = $self->auth_client->post( @post_args );
        }

        if( $self->query_cache ){
            DumpFile( $cache_file, $response );
        }
    }
    return $response;
}


=head1 AUTHOR

Robin Clarke, C<< <perl at robinclarke.net> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-google-fusion at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Google-Fusion>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Google::Fusion


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Google-Fusion>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Google-Fusion>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Google-Fusion>

=item * Search CPAN

L<http://search.cpan.org/dist/Google-Fusion/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2011 Robin Clarke.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of Google::Fusion
