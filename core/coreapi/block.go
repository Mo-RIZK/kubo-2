package coreapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/ipfs/boxo/path"
	pin "github.com/ipfs/boxo/pinning/pinner"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	coreiface "github.com/ipfs/kubo/core/coreiface"
	caopts "github.com/ipfs/kubo/core/coreiface/options"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	util "github.com/ipfs/kubo/blocks/blockstoreutil"
	"github.com/ipfs/kubo/tracing"
)

type BlockAPI CoreAPI

type BlockStat struct {
	path path.ImmutablePath
	size int
}

func (api *BlockAPI) Put(ctx context.Context, src io.Reader, opts ...caopts.BlockPutOption) (coreiface.BlockStat, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.BlockAPI", "Put")
	defer span.End()

	time1 := time.Now()
	settings, err := caopts.BlockPutOptions(opts...)
	if err != nil {
		return nil, err
	}
	time2 := time.Now()
	fmt.Fprintf(os.Stdout, "PUTTTTT Settings : %s \n", time2.Sub(time1).String())
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}
	time3 := time.Now()
	fmt.Fprintf(os.Stdout, "PUTTTTT READDD : %s \n", time3.Sub(time2).String())
	bcid, err := settings.CidPrefix.Sum(data)
	if err != nil {
		return nil, err
	}
	time4 := time.Now()
	fmt.Fprintf(os.Stdout, "PUTTTTT HASHHHHHH : %s \n", time4.Sub(time3).String())
	b, err := blocks.NewBlockWithCid(data, bcid)
	if err != nil {
		return nil, err
	}
	time5 := time.Now()
	fmt.Fprintf(os.Stdout, "PUTTTTT CREATE BLOCKKK : %s \n", time5.Sub(time4).String())
	if settings.Pin {
		defer api.blockstore.PinLock(ctx).Unlock(ctx)
	}

	err = api.blocks.AddBlock(ctx, b)
	if err != nil {
		return nil, err
	}
	time6 := time.Now()
	fmt.Fprintf(os.Stdout, "PUTTTTT STOREEEE : %s \n", time6.Sub(time5).String())
	if settings.Pin {
		if err = api.pinning.PinWithMode(ctx, b.Cid(), pin.Recursive, ""); err != nil {
			return nil, err
		}
		if err := api.pinning.Flush(ctx); err != nil {
			return nil, err
		}
	}

	return &BlockStat{path: path.FromCid(b.Cid()), size: len(data)}, nil
}

func (api *BlockAPI) Get(ctx context.Context, p path.Path) (io.Reader, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.BlockAPI", "Get", trace.WithAttributes(attribute.String("path", p.String())))
	defer span.End()
	rp, _, err := api.core().ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}

	b, err := api.blocks.GetBlock(ctx, rp.RootCid())
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b.RawData()), nil
}

func (api *BlockAPI) Rm(ctx context.Context, p path.Path, opts ...caopts.BlockRmOption) error {
	ctx, span := tracing.Span(ctx, "CoreAPI.BlockAPI", "Rm", trace.WithAttributes(attribute.String("path", p.String())))
	defer span.End()

	rp, _, err := api.core().ResolvePath(ctx, p)
	if err != nil {
		return err
	}

	settings, err := caopts.BlockRmOptions(opts...)
	if err != nil {
		return err
	}
	cids := []cid.Cid{rp.RootCid()}
	o := util.RmBlocksOpts{Force: settings.Force}

	out, err := util.RmBlocks(ctx, api.blockstore, api.pinning, cids, o)
	if err != nil {
		return err
	}

	select {
	case res, ok := <-out:
		if !ok {
			return nil
		}

		remBlock, ok := res.(*util.RemovedBlock)
		if !ok {
			return errors.New("got unexpected output from util.RmBlocks")
		}

		if remBlock.Error != nil {
			return remBlock.Error
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (api *BlockAPI) Stat(ctx context.Context, p path.Path) (coreiface.BlockStat, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.BlockAPI", "Stat", trace.WithAttributes(attribute.String("path", p.String())))
	defer span.End()

	rp, _, err := api.core().ResolvePath(ctx, p)
	if err != nil {
		return nil, err
	}

	b, err := api.blocks.GetBlock(ctx, rp.RootCid())
	if err != nil {
		return nil, err
	}

	return &BlockStat{
		path: path.FromCid(b.Cid()),
		size: len(b.RawData()),
	}, nil
}

func (bs *BlockStat) Size() int {
	return bs.size
}

func (bs *BlockStat) Path() path.ImmutablePath {
	return bs.path
}

func (api *BlockAPI) core() coreiface.CoreAPI {
	return (*CoreAPI)(api)
}
