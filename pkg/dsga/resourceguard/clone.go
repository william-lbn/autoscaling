package resourceguard

// Clone返回完整独立快照；持久恢复证据不能与网络回复或调用者共享slice/
// 指针。这里不重新Validate/解码，便于错误路径保留原输入，不持锁也无网络。
func (v View) Clone() View {
	v.Bootstrap.Limits.HugePools = append(v.Bootstrap.Limits.HugePools[:0:0], v.Bootstrap.Limits.HugePools...)
	for _, field := range []**Command{&v.Original, &v.Held, &v.Cancelled} {
		if *field != nil {
			c := cloneCommand(**field)
			*field = &c
		}
	}
	if v.Birth != nil {
		birth := *v.Birth
		v.Birth = &birth
	}
	v.Actual.HugePools = append(v.Actual.HugePools[:0:0], v.Actual.HugePools...)
	if v.Actual.TaskLimit != nil {
		value := *v.Actual.TaskLimit
		v.Actual.TaskLimit = &value
	}
	if v.Actual.AddressSpace != nil {
		value := *v.Actual.AddressSpace
		v.Actual.AddressSpace = &value
	}
	if v.Actual.System != nil {
		value := *v.Actual.System
		v.Actual.System = &value
	}
	return v
}
