package toshi

import (
	"context"
	"fmt"
	"iter"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// nextPageTokener is a List response: it names the page after it.
type nextPageTokener interface {
	GetNextPageToken() string
}

// Paginate yields every item of a page-at-a-time List call, one page after
// another.
//
// call is the generated List method of a service client, and items picks a
// response's items:
//
//	for project, err := range toshi.Paginate(ctx, &workspacev1.ListProjectsRequest{},
//		client.Project().ListProjects,
//		func(resp *workspacev1.ListProjectsResponse) []*workspacev1.Project {
//			return resp.Projects
//		}) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(project.GetName())
//	}
//
// Paginate sends a copy of req, so the caller's request keeps its fields and
// a page_token it set resumes from that page. It reads each response's
// next_page_token and sends it as the next request's page_token. The sequence
// ends after a page whose next_page_token is empty, when the loop breaks, or
// after an error, which it yields once and then ends. Every page is a call
// through the client, so the read retry policy covers it.
//
// A request type without a page_token field, or a response type without a
// next_page_token field, ends the sequence with an error that names the type.
func Paginate[Req any, Resp any, Item any](
	ctx context.Context,
	req *Req,
	call func(context.Context, *connect.Request[Req]) (*connect.Response[Resp], error),
	items func(*Resp) []Item,
) iter.Seq2[Item, error] {
	return func(yield func(Item, error) bool) {
		var zero Item
		next, setPage, err := clonePageRequest(req)
		if err != nil {
			yield(zero, err)
			return
		}
		for {
			resp, err := call(ctx, connect.NewRequest(next))
			if err != nil {
				yield(zero, err)
				return
			}
			token, ok := any(resp.Msg).(nextPageTokener)
			if !ok {
				yield(zero, fmt.Errorf("toshi: Paginate: %T has no next_page_token field", resp.Msg))
				return
			}
			for _, item := range items(resp.Msg) {
				if !yield(item, nil) {
					return
				}
			}
			if nextPage := token.GetNextPageToken(); nextPage != "" {
				setPage(nextPage)
				continue
			}
			return
		}
	}
}

// clonePageRequest returns a copy of req and a function that sets the copy's
// page_token field, which every page but the first sets.
func clonePageRequest[Req any](req *Req) (*Req, func(string), error) {
	if req == nil {
		return nil, nil, fmt.Errorf("toshi: Paginate: nil request")
	}
	src, ok := any(req).(proto.Message)
	if !ok {
		return nil, nil, fmt.Errorf("toshi: Paginate: %T is not a protocol buffer message", req)
	}
	nextMsg := proto.Clone(src)
	next, ok := any(nextMsg).(*Req)
	if !ok {
		return nil, nil, fmt.Errorf("toshi: Paginate: %T is not a protocol buffer message", req)
	}
	fd := src.ProtoReflect().Descriptor().Fields().ByName("page_token")
	if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() {
		return nil, nil, fmt.Errorf("toshi: Paginate: %T has no page_token field", req)
	}
	setPage := func(token string) {
		nextMsg.ProtoReflect().Set(fd, protoreflect.ValueOfString(token))
	}
	return next, setPage, nil
}
