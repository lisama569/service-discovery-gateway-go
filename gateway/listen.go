package gateway

import "net"

// startListener 在回环地址监听，并把实际地址写回 Server。
func (s *Server) startListener() (net.Listener, error) {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return nil, err
	}
	s.addr = ln.Addr().String()
	return ln, nil
}
