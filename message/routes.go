package message

import (
	"encoding/binary"
	"fmt"
)

// The small codec below follows routes.proto and needs no generated/runtime
// dependency. Unknown protobuf fields are skipped for forward compatibility.
type TopicRoute struct {
	LogicalID    string
	Name         string
	BrokerID     uint16
	TopicID      uint16
	TCPAddr      string
	UDPAddr      string
	OwnerName    string
	HomeBrokerID uint16
}
type BrokerEndpoint struct {
	ID        uint16
	TCPAddr   string
	Available bool
}
type RouteTable struct {
	Version string
	Routes  []TopicRoute
	Brokers []BrokerEndpoint
}
type RoutePage struct {
	Version   string
	Offset    uint64
	Data      []byte
	Done      bool
	Unchanged bool
}

func protoBytes(dst []byte, field uint64, data []byte) []byte {
	dst = binary.AppendUvarint(dst, field<<3|2)
	dst = binary.AppendUvarint(dst, uint64(len(data)))
	return append(dst, data...)
}
func protoUint(dst []byte, field, value uint64) []byte {
	dst = binary.AppendUvarint(dst, field<<3)
	return binary.AppendUvarint(dst, value)
}
func routeBytes(r TopicRoute) []byte {
	var b []byte
	b = protoBytes(b, 1, []byte(r.LogicalID))
	b = protoBytes(b, 2, []byte(r.Name))
	b = protoUint(b, 3, uint64(r.BrokerID))
	b = protoUint(b, 4, uint64(r.TopicID))
	b = protoBytes(b, 5, []byte(r.TCPAddr))
	b = protoBytes(b, 6, []byte(r.UDPAddr))
	b = protoBytes(b, 7, []byte(r.OwnerName))
	return protoUint(b, 8, uint64(r.HomeBrokerID))
}
func (t RouteTable) Encode() []byte {
	b := protoBytes(nil, 1, []byte(t.Version))
	for _, route := range t.Routes {
		b = protoBytes(b, 2, routeBytes(route))
	}
	for _, broker := range t.Brokers {
		v := protoUint(nil, 1, uint64(broker.ID))
		v = protoBytes(v, 2, []byte(broker.TCPAddr))
		if broker.Available {
			v = protoUint(v, 3, 1)
		}
		b = protoBytes(b, 3, v)
	}
	return b
}
func (p RoutePage) Encode() []byte {
	b := protoBytes(nil, 1, []byte(p.Version))
	b = protoUint(b, 2, p.Offset)
	b = protoBytes(b, 3, p.Data)
	if p.Done {
		b = protoUint(b, 4, 1)
	}
	if p.Unchanged {
		b = protoUint(b, 5, 1)
	}
	return b
}

type protoField struct {
	number uint64
	wire   uint64
	data   []byte
	value  uint64
}

func parseProto(data []byte, visit func(protoField) error) error {
	for len(data) > 0 {
		tag, n := binary.Uvarint(data)
		if n <= 0 || tag>>3 == 0 {
			return fmt.Errorf("campo protobuf inválido")
		}
		data = data[n:]
		f := protoField{number: tag >> 3, wire: tag & 7}
		switch f.wire {
		case 0:
			v, n := binary.Uvarint(data)
			if n <= 0 {
				return fmt.Errorf("varint inválido")
			}
			f.value = v
			data = data[n:]
		case 2:
			size, n := binary.Uvarint(data)
			if n <= 0 || size > uint64(len(data)-n) {
				return fmt.Errorf("campo protobuf incompleto")
			}
			data = data[n:]
			f.data = data[:int(size)]
			data = data[int(size):]
		case 1:
			if len(data) < 8 {
				return fmt.Errorf("campo fixo incompleto")
			}
			data = data[8:]
		case 5:
			if len(data) < 4 {
				return fmt.Errorf("campo fixo incompleto")
			}
			data = data[4:]
		default:
			return fmt.Errorf("tipo protobuf não suportado")
		}
		if err := visit(f); err != nil {
			return err
		}
	}
	return nil
}
func decodeRoute(data []byte) (TopicRoute, error) {
	var r TopicRoute
	err := parseProto(data, func(f protoField) error {
		switch f.number {
		case 1, 2, 5, 6, 7:
			if f.wire != 2 {
				return fmt.Errorf("string protobuf inválida")
			}
			switch f.number {
			case 1:
				r.LogicalID = string(f.data)
			case 2:
				r.Name = string(f.data)
			case 5:
				r.TCPAddr = string(f.data)
			case 6:
				r.UDPAddr = string(f.data)
			case 7:
				r.OwnerName = string(f.data)
			}
		case 3, 4, 8:
			if f.wire != 0 || f.value > 65535 {
				return fmt.Errorf("ID de rota inválido")
			}
			if f.number == 3 {
				r.BrokerID = uint16(f.value)
			} else if f.number == 4 {
				r.TopicID = uint16(f.value)
			} else {
				r.HomeBrokerID = uint16(f.value)
			}
		}
		return nil
	})
	return r, err
}
func DecodeRouteTable(data []byte) (RouteTable, error) {
	var table RouteTable
	err := parseProto(data, func(f protoField) error {
		switch f.number {
		case 1:
			if f.wire != 2 {
				return fmt.Errorf("versão inválida")
			}
			table.Version = string(f.data)
		case 2:
			if f.wire != 2 {
				return fmt.Errorf("rota inválida")
			}
			r, err := decodeRoute(f.data)
			if err != nil {
				return err
			}
			table.Routes = append(table.Routes, r)
		case 3:
			if f.wire != 2 {
				return fmt.Errorf("broker inválido")
			}
			var b BrokerEndpoint
			if err := parseProto(f.data, func(v protoField) error {
				switch v.number {
				case 1:
					if v.wire != 0 || v.value > 65535 {
						return fmt.Errorf("ID inválido")
					}
					b.ID = uint16(v.value)
				case 2:
					if v.wire != 2 {
						return fmt.Errorf("endereço inválido")
					}
					b.TCPAddr = string(v.data)
				case 3:
					if v.wire != 0 {
						return fmt.Errorf("disponibilidade inválida")
					}
					b.Available = v.value != 0
				}
				return nil
			}); err != nil {
				return err
			}
			table.Brokers = append(table.Brokers, b)
		}
		return nil
	})
	return table, err
}
func DecodeRoutePage(data []byte) (RoutePage, error) {
	var page RoutePage
	err := parseProto(data, func(f protoField) error {
		switch f.number {
		case 1, 3:
			if f.wire != 2 {
				return fmt.Errorf("bloco inválido")
			}
			if f.number == 1 {
				page.Version = string(f.data)
			} else {
				page.Data = append([]byte(nil), f.data...)
			}
		case 2, 4, 5:
			if f.wire != 0 {
				return fmt.Errorf("posição inválida")
			}
			if f.number == 2 {
				page.Offset = f.value
			} else if f.number == 4 {
				page.Done = f.value != 0
			} else {
				page.Unchanged = f.value != 0
			}
		}
		return nil
	})
	return page, err
}
