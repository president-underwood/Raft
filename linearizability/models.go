package linearizability


type KvInput struct {
	Op uint8 // 0 => get, 1 => put, 2 => append
	Key string
	Value string
}

type KvOutput struct {
	Value string
}

func KvModel() Model {
	return Model {
		Partition: func(history []Operation) [][]Operation {
			m := make(map[string][]Operation)
			for _, v := range history {
				key := v.Input.(KvInput).Key
				m[key] = append(m[key], v)
			}
			var ret [][]Operation
			for _, v := range m {
				ret = append(ret, v)
			}
			return ret
		},
		Init: func() interface{} {
			return ""
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			inp := input.(KvInput)
			out := output.(KvOutput)
			st := state.(string)
			if inp.Op == 0 {
				// 0对应get
				return out.Value == st, state
			} else if inp.Op == 1 {
				// 1对应put
				return true, inp.Value
			} else {
				// 其他情况则是append
				return true, (st + inp.Value)
			}
		},
		Equal: ShallowEqual,
	}
}
