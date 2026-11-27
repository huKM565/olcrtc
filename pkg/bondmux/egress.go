package bondmux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"
)

var (
	ErrBlockedTarget = errors.New("bondmux: blocked target")
	ErrResolveTarget = errors.New("bondmux: resolve target failed")
)

var blockedRanges = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/3"),
	netip.MustParsePrefix("::/127"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

func isBlockedAddr(addr netip.Addr) bool {
	addr = addr.Unmap()
	if !addr.IsValid() || addr.Zone() != "" {
		return true
	}
	for _, p := range blockedRanges {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

type egressDialer struct {
	allowLoopback bool
	dnsServer     string
	lookupIP      func(ctx context.Context, host string) ([]netip.Addr, error)
	dialTCPFn     func(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error)
	resolver      *net.Resolver
}

func newEgressDialer(
	dnsServer string,
	allowLoopback bool,
	lookupIP func(ctx context.Context, host string) ([]netip.Addr, error),
	dialTCPFn func(ctx context.Context, addr netip.Addr, port uint16) (net.Conn, error),
) *egressDialer {
	d := &egressDialer{
		allowLoopback: allowLoopback,
		dnsServer:     dnsServer,
		lookupIP:      lookupIP,
		dialTCPFn:     dialTCPFn,
	}
	if dnsServer != "" {
		d.resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var nd net.Dialer
				nd.Timeout = 4 * time.Second
				return nd.DialContext(ctx, network, dnsServer)
			},
		}
	}
	return d
}

func (d *egressDialer) resolve(ctx context.Context, dst TargetAddr) ([]netip.Addr, error) {
	if dst.Host == "" || dst.Port == 0 {
		return nil, ErrInvalidAddr
	}
	if ip, err := netip.ParseAddr(dst.Host); err == nil {
		return d.filter([]netip.Addr{ip})
	}
	var addrs []netip.Addr
	if d.lookupIP != nil {
		var err error
		addrs, err = d.lookupIP(ctx, dst.Host)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrResolveTarget, err)
		}
	} else {
		res := d.resolver
		if res == nil {
			res = net.DefaultResolver
		}
		ips, err := res.LookupIPAddr(ctx, dst.Host)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrResolveTarget, err)
		}
		for _, ip := range ips {
			if a, ok := netip.AddrFromSlice(ip.IP); ok {
				addrs = append(addrs, a)
			}
		}
	}
	if len(addrs) == 0 {
		return nil, ErrResolveTarget
	}
	return d.filter(addrs)
}

func (d *egressDialer) filter(addrs []netip.Addr) ([]netip.Addr, error) {
	out := make([]netip.Addr, len(addrs))
	for i, a := range addrs {
		u := a.Unmap()
		if isBlockedAddr(u) && !(d.allowLoopback && u.IsLoopback()) {
			return nil, ErrBlockedTarget
		}
		out[i] = u
	}
	return out, nil
}

func (d *egressDialer) dialTCP(ctx context.Context, dst TargetAddr) (net.Conn, error) {
	addrs, err := d.resolve(ctx, dst)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, a := range addrs {
		if d.dialTCPFn != nil {
			c, err := d.dialTCPFn(ctx, a, dst.Port)
			if err == nil {
				return c, nil
			}
			lastErr = err
			continue
		}
		var nd net.Dialer
		nd.Timeout = 10 * time.Second
		c, err := nd.DialContext(ctx, "tcp", net.JoinHostPort(a.String(), strconv.Itoa(int(dst.Port))))
		if err == nil {
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			return c, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = ErrResolveTarget
	}
	return nil, lastErr
}

func (d *egressDialer) resolveUDP(ctx context.Context, dst TargetAddr) (*net.UDPAddr, error) {
	addrs, err := d.resolve(ctx, dst)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{IP: addrs[0].AsSlice(), Port: int(dst.Port)}, nil
}
