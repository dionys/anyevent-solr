package AnyEvent::Solr;

use strict;
use warnings;

use AnyEvent ();
use AnyEvent::HTTP qw(http_post);
use JSON qw(encode_json decode_json);

use namespace::clean;


our $VERSION = '0.01';


sub new {
	my ($pkg, %args) = @_;

	my $self = bless({}, ref($pkg) || $pkg);

	$self->{url}        = $args{url} || 'http://127.0.0.1:8983/solr';
	$self->{core}       = $args{core};
	$self->{autocommit} = exists($args{autocommit}) ? !!$args{autocommit} : 1;

	return $self;
}

sub add {
	my $cb = pop() if ref($_[-1]) eq 'CODE';
	my ($self, $docs, %opts) = @_;

	my %pars;
	my $com = $self->_autocommit(\%opts);

	$pars{commit}       = ($com ? 'true' : 'false') if defined($com);
	$pars{commitWithin} = $opts{commit_within} * 1000 if exists($opts{commit_within});

	$docs = [$docs] unless ref($docs) eq 'ARRAY';

	my $data = [grep { defined() && ref($_) eq 'HASH' } @$docs];

	$self->_update(\%pars, $data, $cb);

	return;
}

*update = \&add;

sub delete {
	my $cb = pop() if ref($_[-1]) eq 'CODE';
	my ($self, $cond, %opts) = @_;

	my %pars;
	my $com = $self->_autocommit(\%opts);

	$pars{commit} = ($com ? 'true' : 'false') if defined($com);

	my @args;

	if (defined($cond)) {
		my $ref = ref($cond);

		if ($ref eq 'HASH') {
			for my $key (qw(id query)) {
				next unless exists($cond->{$key});
				push(@args, {$key => $_}) for ref($cond->{$key}) eq 'ARRAY' ? @{$cond->{$key}} : $cond->{$key};
			}
		}
		elsif ($ref eq 'ARRAY') {
			push(@args, {id => $_}) for @$cond;
		}
		elsif (!$ref) {
			push(@args, {id => $cond});
		}
	}

	$self->_update(\%pars, {delete => \@args}, $cb);

	return;
}

sub delete_by_id {
	shift()->delete({id => shift()}, @_);
}

sub delete_by_query {
	shift()->delete({query => shift()}, @_);
}

sub commit {
	my $cb = pop() if ref($_[-1]) eq 'CODE';
	my ($self, %opts) = @_;

	my %args;

	$args{expungeDeletes} = $opts{expunge_deletes} ? \1 : \0 if exists($opts{expunge_deletes});
	$args{softCommit}     = $opts{soft_commit}     ? \1 : \0 if exists($opts{soft_commit});
	$args{waitFlush}      = $opts{wait_flush}      ? \1 : \0 if exists($opts{wait_flush});
	$args{waitSearcher}   = $opts{wait_searcher}   ? \1 : \0 if exists($opts{wait_searcher});

	$self->_update({}, {commit => \%args}, $cb);

	return;
}

sub rollback {
	my $cb = pop() if ref($_[-1]) eq 'CODE';
	my ($self) = @_;

	$self->_update({}, {rollback => {}}, $cb);

	return;
}

sub search {
	my $cb = pop() if ref($_[-1]) eq 'CODE';
	my ($self, $qry, %opts) = @_;

	$opts{q} = $qry;

	$self->_request(
		$self->_url('select'),
		{params => \%opts},
		sub {
			$cb->(
				$_[1]
				    ? undef
				    : ref($_[0]{response}{docs}) eq 'ARRAY'
				    ? $_[0]{response}{docs}
				    : $_[0]{response}{docs}
				    ? [$_[0]{response}{docs}]
				    : [],
				@_[0 .. 2]
			) if $cb;
		}
	);

	return;
}

sub _autocommit {
	my ($self, $opts) = @_;

	my $val;

	$val = $self->{autocommit} if $self->{autocommit};
	$val = $opts->{commit}     if exists($opts->{commit});

	return $val;
}

sub _request {
	my ($self, $url, $data, $cb) = @_;

	http_post(
		$url,
		encode_json($data),
		headers   => {'Content-Type' => 'application/json; charset=utf-8'},
		keepalive => 1,
		sub {
			$_[1]->{RequestData} = $data;
			$self->_response($_[0], $_[1], $cb);
		}
	);

	return;
}

sub _response {
	my ($self, $body, $hdrs, $cb) = @_;

	$hdrs->{ResponseBody} = $body;

	my $data;
	my $err;
	my $code = $hdrs->{Status};

	if ($code >= 590) {
		$err = sprintf('Connection error: %s', $hdrs->{Reason});
	}
	elsif ($code != 200 && $code != 400 && $code != 500) {
		$err = sprintf('HTTP error: %d - %s', $hdrs->{Status}, $hdrs->{Reason});
	}
	unless ($err) {
		$data = eval { decode_json($body) };
		$err = sprintf('Response decoding error: %s', $@) if $@;
	}
	unless ($err && $data->{responseHeader}{status} != 0) {
		$err = sprintf('Solr error: %s - %s', $data->{error}{code} || 0, $data->{error}{msg} || '');
	}

	$cb->($data, $err, $hdrs);

	return;
}

sub _update {
	my ($self, $pars, $data, $cb) = @_;

	$self->_request(
		$self->_url('update', $pars),
		$data,
		sub {
			$cb->($_[1] ? undef : 1, @_[0 .. 2]) if $cb;
		}
	);

	return;
}

sub _url {
	my ($self, $act, $pars) = @_;

	$pars ||= {};
	$pars->{wt} = 'json';

	return join('/', grep { $_ } $self->{url}, $self->{core}, $act)
	    . '?' . join('&', map { $_ . '=' . $pars->{$_}  } keys(%$pars));
}


1;
