package bondmux

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

const (
	dnsCacheTTL         = 30 * time.Second
	dnsCacheMaxEntries  = 1024
	happyEyeballsDelay  = 250 * time.Millisecond
	maxDialCandidates   = 4
)

type dnsCacheEntry struct {
	addrs     []netip.Addr
	expiresAt time.Time
}

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
	dnsMu         sync.RWMutex
	dnsCache      map[string]dnsCacheEntry
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
		dnsCache:      make(map[string]dnsCacheEntry),
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

func (d *egressDialer) lookupCached(host string) ([]netip.Addr, bool) {
	d.dnsMu.RLock()
	ent, ok := d.dnsCache[host]
	d.dnsMu.RUnlock()
	if !ok || time.Now().After(ent.expiresAt) {
		return nil, false
	}
	return ent.addrs, true
}

func (d *egressDialer) storeCached(host string, addrs []netip.Addr) {
	now := time.Now()
	d.dnsMu.Lock()
	if len(d.dnsCache) >= dnsCacheMaxEntries {
		for k, v := range d.dnsCache {
			if now.After(v.expiresAt) {
				delete(d.dnsCache, k)
			}
		}
		if len(d.dnsCache) >= dnsCacheMaxEntries {
			for k := range d.dnsCache {
				delete(d.dnsCache, k)
				break
			}
		}
	}
	d.dnsCache[host] = dnsCacheEntry{
		addrs:     append([]netip.Addr(nil), addrs...),
		expiresAt: now.Add(dnsCacheTTL),
	}
	d.dnsMu.Unlock()
}

func (d *egressDialer) resolve(ctx context.Context, dst TargetAddr) ([]netip.Addr, error) {
	if dst.Host == "" || dst.Port == 0 {
		return nil, ErrInvalidAddr
	}
	if ip, err := netip.ParseAddr(dst.Host); err == nil {
		return d.filter([]netip.Addr{ip})
	}
	if cached, ok := d.lookupCached(dst.Host); ok {
		return cached, nil
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
	filtered, err := d.filter(addrs)
	if err != nil {
		return nil, err
	}
	d.storeCached(dst.Host, filtered)
	return filtered, nil
}

func (d *egressDialer) filter(addrs []netip.Addr) ([]netip.Addr, error) {
	v4 := make([]netip.Addr, 0, len(addrs))
	v6 := make([]netip.Addr, 0, len(addrs))
	for _, a := range addrs {
		u := a.Unmap()
		if isBlockedAddr(u) && !(d.allowLoopback && u.IsLoopback()) {
			return nil, ErrBlockedTarget
		}
		if u.Is4() {
			v4 = append(v4, u)
		} else {
			v6 = append(v6, u)
		}
	}
	return append(v4, v6...), nil
}

func (d *egressDialer) dialOneTCP(ctx context.Context, a netip.Addr, port uint16) (net.Conn, error) {
	if d.dialTCPFn != nil {
		return d.dialTCPFn(ctx, a, port)
	}
	var nd net.Dialer
	nd.Timeout = 8 * time.Second
	c, err := nd.DialContext(ctx, "tcp", net.JoinHostPort(a.String(), strconv.Itoa(int(port))))
	if err == nil {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
	}
	return c, err
}

func (d *egressDialer) dialTCP(ctx context.Context, dst TargetAddr) (net.Conn, error) {
	addrs, err := d.resolve(ctx, dst)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 1 {
		return d.dialOneTCP(ctx, addrs[0], dst.Port)
	}
	if len(addrs) > maxDialCandidates {
		addrs = addrs[:maxDialCandidates]
	}

	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type dialResult struct {
		conn net.Conn
		err  error
	}
	resCh := make(chan dialResult, len(addrs))
	launch := func(a netip.Addr) {
		go func() {
			c, err := d.dialOneTCP(dialCtx, a, dst.Port)
			if err == nil {
				select {
				case resCh <- dialResult{conn: c}:
				case <-dialCtx.Done():
					_ = c.Close()
				}
				return
			}
			resCh <- dialResult{err: err}
		}()
	}

	launched := 0
	completed := 0
	var lastErr error
	launch(addrs[launched])
	launched++

	stagger := time.NewTimer(happyEyeballsDelay)
	defer stagger.Stop()

	for completed < len(addrs) {
		if launched < len(addrs) {
			select {
			case r := <-resCh:
				completed++
				if r.err == nil {
					cancel()
					return r.conn, nil
				}
				lastErr = r.err
				launch(addrs[launched])
				launched++
				stagger.Reset(happyEyeballsDelay)
			case <-stagger.C:
				launch(addrs[launched])
				launched++
				if launched < len(addrs) {
					stagger.Reset(happyEyeballsDelay)
				}
			case <-ctx.Done():
				cancel()
				return nil, ctx.Err()
			}
		} else {
			select {
			case r := <-resCh:
				completed++
				if r.err == nil {
					cancel()
					return r.conn, nil
				}
				lastErr = r.err
			case <-ctx.Done():
				cancel()
				return nil, ctx.Err()
			}
		}
	}
	if lastErr == nil {
		lastErr = ErrResolveTarget
	}
	return nil, lastErr
}

func (d *egressDialer) resolveUDPFast(dst TargetAddr) (*net.UDPAddr, bool) {
	if dst.Host == "" || dst.Port == 0 {
		return nil, false
	}
	if ip, err := netip.ParseAddr(dst.Host); err == nil {
		filtered, err := d.filter([]netip.Addr{ip})
		if err != nil || len(filtered) == 0 {
			return nil, false
		}
		return &net.UDPAddr{IP: filtered[0].AsSlice(), Port: int(dst.Port)}, true
	}
	if cached, ok := d.lookupCached(dst.Host); ok && len(cached) > 0 {
		return &net.UDPAddr{IP: cached[0].AsSlice(), Port: int(dst.Port)}, true
	}
	return nil, false
}

func (d *egressDialer) resolveUDP(ctx context.Context, dst TargetAddr) (*net.UDPAddr, error) {
	addrs, err := d.resolve(ctx, dst)
	if err != nil {
		return nil, err
	}
	return &net.UDPAddr{IP: addrs[0].AsSlice(), Port: int(dst.Port)}, nil
}
