package twitter

type Tweet struct {
	Message string `protobuf:"bytes,1,opt,name=message,proto3"
	json:"message,omitempty"`
}
