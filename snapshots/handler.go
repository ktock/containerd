/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package snapshots

import (
	"context"

	"github.com/containerd/containerd/images"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// TargetSnapshotLabel is a label to tell the backend snapshotter the
	// chainID of this snapshot. The backend snapshotter can use this
	// information to search for the contents of the snapshot and to commit
	// it. The client can use the committed snapshot with the chainID later.
	TargetSnapshotLabel = "containerd.io/snapshot.ref"

	// TargetManifestLabel is a label to tell the backend snapshotter the basic
	// information(manifest digest) of the image layer. Backend snapshotters can
	// use it for searching/preparing snapshot contents.
	TargetManifestLabel = "containerd.io/snapshot/target.manifest"

	// TargetRefLabel is a label to tell the backend snapshotter the basic
	// information(image reference) of the image layer. Backend snapshotters can
	// use it for searching/preparing snapshot contents.
	TargetRefLabel = "containerd.io/snapshot/target.reference"

	// TargetDigestLabel is a label to tell the backend snapshotter the
	// basic information(layer digest) of the image layer. Backend snapshotters can
	// use it for searching/preparing snapshot contents.
	TargetDigestLabel = "containerd.io/snapshot/target.digest"
)

// AppendImageInfoHandlerWrapper appends image's basic information to each layer
// descriptor as annotations which will be passed to backend snapshotters as
// labels through unpacker and used by them to search/prepare layers.
func AppendImageInfoHandlerWrapper(f images.Handler, ref string) images.Handler {
	return images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		children, err := f.Handle(ctx, desc)
		if err != nil {
			return nil, err
		}
		switch desc.MediaType {
		case ocispec.MediaTypeImageManifest, images.MediaTypeDockerSchema2Manifest:
			for i, _ := range children {
				c := &children[i]
				if images.IsLayerType(c.MediaType) {
					if c.Annotations == nil {
						c.Annotations = make(map[string]string)
					}
					c.Annotations[TargetManifestLabel] = desc.Digest.String()
					c.Annotations[TargetRefLabel] = ref
					c.Annotations[TargetDigestLabel] = c.Digest.String()
				}
			}
		}
		return children, nil
	})
}
