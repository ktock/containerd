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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	RemoteSnapshotLabel string = "containerd.io/snapshot/remote_snapshot"
	RemoteRefLabel      string = "containerd.io/snapshot/remote_snapshot/ref"
	RemoteDigestLabel   string = "containerd.io/snapshot/remote_snapshot/digest"
)

// FilterLayerBySnapshotter filters out layers from download candidates if we
// can make a snapshot without downloading the actual contents of the layer.
func FilterLayerBySnapshotter(f images.HandlerFunc, sn Snapshotter, store content.Store, fetcher remotes.Fetcher, ref string) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		children, err := f(ctx, desc)
		if err != nil {
			return nil, err
		}

		if desc.MediaType == ocispec.MediaTypeImageManifest ||
			desc.MediaType == images.MediaTypeDockerSchema2Manifest {
			p, err := content.ReadBlob(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			var manifest ocispec.Manifest
			if err := json.Unmarshal(p, &manifest); err != nil {
				return nil, err
			}

			configDesc := manifest.Config
			if _, err := remotes.FetchHandler(store, fetcher)(ctx, configDesc); err != nil {
				return nil, err
			}
			configLayers, err := images.RootFS(ctx, store, configDesc)
			if err != nil {
				return nil, err
			}

			necessary, _, _ := createRemoteChain(ctx, manifest.Layers, configLayers, sn, ref)
			unnecessary := exclude(manifest.Layers, necessary)
			children = exclude(children, unnecessary)
		}

		return children, nil
	}
}

// createRemoteChain checks if each layer and the lower layers are "remote
// layer" with which the remote snapshotter can make a snapshot without
// downloading the actual layer contents.
// If so, it filters out the layer from download candidates and make the
// snapshot(we call "remote snapshot" here) NOW to avoid unpacking the
// layer contents.
func createRemoteChain(ctx context.Context, layers []ocispec.Descriptor, diffIDs []digest.Digest, sn Snapshotter, ref string) ([]ocispec.Descriptor, string, bool) {
	if len(layers) <= 0 {
		return nil, "", true
	}
	chainID := identity.ChainID(diffIDs).String()

	// Make sure that all lower chains are remote snapshots.
	necessary, parentID, ok := createRemoteChain(ctx, layers[:len(layers)-1], diffIDs[:len(diffIDs)-1], sn, ref)
	if !ok {

		// Some of lower chains aren't remote snapshots.
		// We need to fetch all layers above.
		return append(necessary, layers[len(layers)-1]), chainID, false
	}

	if info, err := sn.Stat(ctx, chainID); err == nil {

		// The snapshot is applied a special label "RemoteSnapshotLabel".
		// This label is automatically applied by the remote snapshotter
		// if the snapshot is a remote snapshot.
		if _, ok := info.Labels[RemoteSnapshotLabel]; ok {

			// Snapshotter is remote snapshotter and the remote
			// snapshot already exists. We avoid to download it.
			return necessary, chainID, true
		}

		// Snapshotter is not a remote snapshotter or the snapshot
		// isn't remote snapshot. We need to fetch all layers above.
		return append(necessary, layers[len(layers)-1]), chainID, false
	}

	// We got error during Stat(), so the snapshot hasn't been made yet.
	//
	// Following cases are possible:
	// A. Snapshotter is a remote snapshotter and the layer is a remote
	//    layer.
	// B. Snapshotter is a remote snapshotter and the layer isn't a remote
	//    layer.
	// C. Snapshotter is a normal snapshotter.
	//
	// Only in the case of A, we want the remote snapshotter to make the
	// remote snapshot NOW and skip downloading the layer by filter out the
	// layer. To achive that, we need to:
	// 1. know that the underlyeing snapshotter is a remote snapshotter, and
	// 2. make the remote snapshot NOW if the layer is a remote layer.
	//
	// We acheve that by using Prepare(), Stat() and Commit() with special
	// labels.
	// The reason why we manually invoke Prepare() and Commit() is we want
	// containerd to recognise proper metadata which is binded to the
	// current namespace. It can't be achived with automatic snapshot
	// generation in the remote snapshotter internally.

	// 1. Prepare()ing a snapshot with passing basic information about this
	//    layer (ref and layer digest) as labels. Remote snapshotters MUST
	//    recognise these labels and MUST check if the layer is a remote
	//    layer. If the remote snapshot exists, remote snapshotter MUST
	//    prepare the active snapshot WITH automatically applying a label
	//    "RemoteSnapshotLabel".
	remoteOpt := WithLabels(map[string]string{
		RemoteRefLabel:    ref,
		RemoteDigestLabel: layers[len(layers)-1].Digest.String(),
	})
	key := fmt.Sprintf("remote-%s %s", uniquePart(), chainID)
	if _, err := sn.Prepare(ctx, key, parentID, remoteOpt); err == nil {

		// 2. Then we Stat() the prepared active snapshot. If the active
		//    snapshot has a RemoteSnapshotLabel, it means we are in the case of
		//    A(mentioned above). So we can safely Commit() the remote snapshot
		//    without any opration on the active snapshot and skip downloading
		//    this layer.
		//    Through these steps, we don't explicitly apply RemoteSnapshotLabels
		//    to any snapshots. This label is applied only in the remote
		//    snapshotter fully automatically. So we can use this label to know
		//    that the underlying snapshotters is a remote snapshotters or not.
		if info, err := sn.Stat(ctx, key); err == nil {
			if _, ok := info.Labels[RemoteSnapshotLabel]; ok {

				// 3. The remote snapshot has a label RemoteSnapshotLabel which
				//    we haven't applied above, it means the snapshotter is a remote
				//    snapshotter and this layer is a remote layer. So we don't do
				//    any operation on the active snapshot and simply Commit() it.
				//    When Commit()-ing a remote snapshot, remote snapshotter MUST
				//    recognise RemoteSnapshotLabel applied to the corresponding active
				//    snapshot and MUST apply the RemoteSnapshotLabel to the
				//    corresponding commiting snapshot automatically.
				if err := sn.Commit(ctx, chainID, key); err == nil {

					// We succeeded to Commit() the remote snapshot.
					// Now, we can safely skip to download the layer.
					return necessary, chainID, true
				}
			}
		}
	}

	// We failed to make the remote snapshotter, so we treat this layer as a
	// normal way.
	sn.Remove(ctx, key)
	return append(necessary, layers[len(layers)-1]), chainID, false
}

func uniquePart() string {
	t := time.Now()
	var b [3]byte
	// Ignore read failures, just decreases uniqueness
	rand.Read(b[:])
	return fmt.Sprintf("%d-%s", t.Nanosecond(), base64.URLEncoding.EncodeToString(b[:]))
}

func exclude(a []ocispec.Descriptor, b []ocispec.Descriptor) []ocispec.Descriptor {
	amap := map[string]ocispec.Descriptor{}
	for _, va := range a {
		amap[va.Digest.String()] = va
	}
	for _, vb := range b {
		delete(amap, vb.Digest.String())
	}
	var res []ocispec.Descriptor
	for _, va := range amap {
		res = append(res, va)
	}
	return res
}
