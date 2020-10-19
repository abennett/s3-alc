package main

import (
	"bufio"
	"bytes"
	"io"
)

// 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be awsexamplebucket1 [06/Feb/2019:00:00:38 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - "GET /awsexamplebucket1?versioning HTTP/1.1" 200 - 113 - 7 - "-" "S3Console/0.4" - s9lzHYrFp76ZVxRcpX9+5cjAnEH2ROuNkd2BHfIa6UkFVdtjf5mKR3/eTPFvsiP/XV/VLi31234= SigV2 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader awsexamplebucket1.s3.us-west-1.amazonaws.com TLSV1.1

type Token int

const (
	ILLEGAL Token = iota
	FIELD
	NL
	EOF

	eof = rune(0)
)

func (t Token) String() string {
	switch t {
	case ILLEGAL:
		return "ILLEGAL"
	case FIELD:
		return "FIELD"
	case NL:
		return "NL"
	default:
		return "UNKNOWN"
	}
}

func isWhiteSpace(ch rune) bool {
	return ch == ' ' || ch == '\t'
}

type Scanner struct {
	r *bufio.Reader
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r: bufio.NewReader(r),
	}
}

func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return rune(0)
	}
	return ch
}

func (s *Scanner) unread() {
	s.r.UnreadRune()
}

func (s *Scanner) Scan() (t Token, lit string) {
	for {
		ch := s.read()
		if ch == eof {
			return EOF, ""
		}
		if isWhiteSpace(ch) {
			continue
		}
		if ch == '\n' {
			return NL, string(ch)
		}
		if ch == '"' || ch == '[' {
			return s.scanQuoteBracket()
		}
		s.unread()
		return s.scanCharacters()
	}
}

func (s *Scanner) ScanLine() ([]string, bool) {
	var line []string
	for {
		tok, lit := s.Scan()
		switch tok {
		case EOF:
			return line, true
		case NL:
			if len(line) > 0 {
				return line, false
			}
		default:
			line = append(line, lit)
		}
	}
}

func (s *Scanner) scanCharacters() (t Token, lit string) {
	var b bytes.Buffer
	b.WriteRune(s.read())
	for {
		ch := s.read()
		switch {
		case ch == eof:
			return EOF, ""
		case isWhiteSpace(ch), ch == '\n':
			s.unread()
			return FIELD, b.String()
		default:
			b.WriteRune(ch)
		}
	}
}

func (s *Scanner) scanQuoteBracket() (t Token, lit string) {
	var b bytes.Buffer
	for {
		ch := s.read()
		switch ch {
		case eof:
			return ILLEGAL, b.String()
		case '"', ']':
			return FIELD, b.String()
		default:
			b.WriteRune(ch)
		}
	}
}
