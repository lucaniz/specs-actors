package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadlines(t *testing.T) {
	sectors := []*miner.SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),

		testSector(11, 5, 54, 64, 1004),
		testSector(13, 6, 55, 65, 1005),
		testSector(8, 7, 56, 66, 1006),
		testSector(8, 8, 57, 67, 1007),

		testSector(8, 9, 58, 68, 1008),
	}

	sectorSize := abi.SectorSize(32 << 30)
	quantSpec := miner.NewQuantSpec(4, 1)
	partitionSize := uint64(4)
	builder := mock.NewBuilder(context.Background(), address.Undef)

	t.Run("adds sectors", func(t *testing.T) {
		rt := builder.Build(t)
		store := adt.AsStore(rt)

		dl := emptyDeadline(t, rt)
		power, err := dl.AddSectors(store, partitionSize, sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))
		assertDeadlineState(t, rt,
			dl,
			sectors,
			sectorSize,
			partitionSize,
			bf(), // faults
			bf(), // recovering
			bf(), // terminations
			bf(), // posts

			bf(1, 2, 3, 4),
			bf(5, 6, 7, 8),
			bf(9),
		)
	})

	t.Run("terminates sectors", func(t *testing.T) {
		rt := builder.Build(t)
		store := adt.AsStore(rt)

		dl := emptyDeadline(t, rt)

		// Add sectors.
		{
			_, err := dl.AddSectors(store, partitionSize, sectors, sectorSize, quantSpec)
			require.NoError(t, err)

			assertDeadlineState(t, rt,
				dl,
				sectors,
				sectorSize,
				partitionSize,
				bf(), // faults
				bf(), // recovering
				bf(), // terminations
				bf(), // posts

				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			)
		}

		// Terminate sectors in partitions 0 and 1, mark sector 9 faulty.
		// Unfortunately, we don't have per-deadline methods for stuff
		// like this.
		{
			partitions, err := dl.PartitionsArray(store)
			require.NoError(t, err)

			var part miner.Partition

			// terminate sectors 1, 3 in partition 0
			{
				found, err := partitions.Get(0, &part)
				require.NoError(t, err)
				require.True(t, found)

				_, err = part.TerminateSectors(store, 15, selectSectors(t, sectors, bf(1, 3)), sectorSize, quantSpec)
				require.NoError(t, err)

				err = partitions.Set(0, &part)
				require.NoError(t, err)

				dl.EarlyTerminations.Set(0)
				dl.LiveSectors -= 2
			}

			// terminate sector 6 in partition 1
			{
				found, err := partitions.Get(1, &part)
				require.NoError(t, err)
				require.True(t, found)

				_, err = part.TerminateSectors(store, 15, selectSectors(t, sectors, bf(6)), sectorSize, quantSpec)
				require.NoError(t, err)

				err = partitions.Set(1, &part)
				require.NoError(t, err)

				dl.EarlyTerminations.Set(1)
				dl.LiveSectors -= 1
			}

			// mark partition 3 faulty
			{
				found, err := partitions.Get(2, &part)
				require.NoError(t, err)
				require.True(t, found)

				_, _, err = part.RecordMissedPost(store, 17, quantSpec)
				require.NoError(t, err)

				err = partitions.Set(2, &part)
				require.NoError(t, err)

				err = dl.AddExpirationPartitions(store, 17, []uint64{2}, quantSpec)
				require.NoError(t, err)
			}

			dl.Partitions, err = partitions.Root()
			require.NoError(t, err)

			assertDeadlineState(t, rt,
				dl,
				sectors,
				sectorSize,
				partitionSize,
				bf(9),       // faults
				bf(),        // recovering
				bf(1, 3, 6), // terminations
				bf(),        // posts

				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			)
		}

		// Try to remove a partition with early terminations.
		{
			_, _, _, err := dl.RemovePartitions(store, bf(0), quantSpec)
			require.Error(t, err, "should have failed to remove a partition with early terminations")
		}

		// Pop early terminations
		{
			earlyTerminations, more, err := dl.PopEarlyTerminations(store, 100, 100)
			require.NoError(t, err)
			assert.False(t, more)
			assert.Equal(t, uint64(2), earlyTerminations.PartitionsProcessed)
			assert.Equal(t, uint64(3), earlyTerminations.SectorsProcessed)
			assert.Len(t, earlyTerminations.Sectors, 1)
			assertBitfieldEquals(t, earlyTerminations.Sectors[15], 1, 3, 6)

			// Popping early terminations doesn't affect the terminations bitfield.
			assertDeadlineState(t, rt,
				dl,
				sectors,
				sectorSize,
				partitionSize,
				bf(9),       // faults
				bf(),        // recovering
				bf(1, 3, 6), // terminations
				bf(),        // posts

				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			)
		}

		// Try to remove no partitions
		{
			live, dead, removedPower, err := dl.RemovePartitions(store, bf(), quantSpec)
			require.NoError(t, err, "should not have failed to remove no partitions")
			require.True(t, removedPower.IsZero())
			assertBitfieldEquals(t, live)
			assertBitfieldEquals(t, dead)

			// Popping early terminations doesn't affect the terminations bitfield.
			assertDeadlineState(t, rt,
				dl,
				sectors,
				sectorSize,
				partitionSize,
				bf(9),       // faults
				bf(),        // recovering
				bf(1, 3, 6), // terminations
				bf(),        // posts

				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			)
		}

		// Remove some partitions
		{
			live, dead, removedPower, err := dl.RemovePartitions(store, bf(0), quantSpec)
			require.NoError(t, err, "should have removed partitions")
			assertBitfieldEquals(t, live, 2, 4)
			assertBitfieldEquals(t, dead, 1, 3)
			livePower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, live))
			require.True(t, livePower.Equals(removedPower))

			assertDeadlineState(t, rt,
				dl,
				sectors,
				sectorSize,
				partitionSize,
				bf(9), // faults
				bf(),  // recovering
				bf(6), // terminations
				bf(),  // posts

				bf(5, 6, 7, 8),
				bf(9),
			)
		}

		// Try to remove a partition with faulty sectors.
		{
			_, _, _, err := dl.RemovePartitions(store, bf(1), quantSpec)
			require.Error(t, err, "should have failed to remove a partition with faults")
		}

		// Try to remove a missing partition.
		{
			_, _, _, err := dl.RemovePartitions(store, bf(2), quantSpec)
			require.Error(t, err, "should have failed to remove missing partition")
		}
	})
}

func emptyDeadline(t *testing.T, rt *mock.Runtime) *miner.Deadline {
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	return miner.ConstructDeadline(root)
}

func assertDeadlineState(
	t *testing.T,
	rt *mock.Runtime,
	dl *miner.Deadline,
	sectors []*miner.SectorOnChainInfo,
	sectorSize abi.SectorSize,
	partitionSize uint64,

	faults *bitfield.BitField,
	recovering *bitfield.BitField,
	terminations *bitfield.BitField,
	posts *bitfield.BitField,

	partitionSectors ...*bitfield.BitField,
) {
	store := adt.AsStore(rt)
	partitions, err := dl.PartitionsArray(store)
	require.NoError(t, err)

	require.Equal(t, uint64(len(partitionSectors)), partitions.Length())

	expectedDeadlineExpQueue := make(map[abi.ChainEpoch][]uint64)
	var partitionsWithEarlyTerminations []uint64

	expectPartIndex := int64(0)
	var partition miner.Partition
	err = partitions.ForEach(&partition, func(partIdx int64) error {
		require.Equal(t, expectPartIndex, partIdx)
		expectPartIndex++

		partSectorNos := partitionSectors[partIdx]

		partFaults, err := bitfield.IntersectBitField(faults, partSectorNos)
		require.NoError(t, err)

		partRecovering, err := bitfield.IntersectBitField(recovering, partSectorNos)
		require.NoError(t, err)

		partTerminations, err := bitfield.IntersectBitField(terminations, partSectorNos)
		require.NoError(t, err)

		unterminatedSectors, err := bitfield.SubtractBitField(partSectorNos, partTerminations)
		require.NoError(t, err)
		partSectors := selectSectors(t, sectors, unterminatedSectors)

		assertPartitionState(t,
			&partition, partSectors, sectorSize, partSectorNos, partFaults, partRecovering, partTerminations)

		earlyTerminated, err := adt.AsArray(store, partition.EarlyTerminated)
		require.NoError(t, err)
		if earlyTerminated.Length() > 0 {
			partitionsWithEarlyTerminations = append(partitionsWithEarlyTerminations, uint64(partIdx))
		}

		// The partition's expiration queue is already tested by the
		// partition tests.
		//
		// Here, we're making sure it's consistent with the deadline's queue.
		q, err := adt.AsArray(store, partition.ExpirationsEpochs)
		require.NoError(t, err)
		err = q.ForEach(nil, func(epoch int64) error {
			expectedDeadlineExpQueue[abi.ChainEpoch(epoch)] = append(
				expectedDeadlineExpQueue[abi.ChainEpoch(epoch)],
				uint64(partIdx),
			)
			return nil
		})
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	allSectors, err := bitfield.MultiMerge(partitionSectors...)
	require.NoError(t, err)

	allSectorsCount, err := allSectors.Count()
	require.NoError(t, err)

	deadSectorsCount, err := terminations.Count()
	require.NoError(t, err)

	require.Equal(t, dl.LiveSectors, allSectorsCount-deadSectorsCount)
	require.Equal(t, dl.TotalSectors, allSectorsCount)

	assertBitfieldsEqual(t, dl.PostSubmissions, posts)

	// Validate expiration queue. The deadline expiration queue is a
	// superset of the partition expiration queues because we never remove
	// from it.
	{
		expirationEpochs, err := adt.AsArray(store, dl.ExpirationsEpochs)
		require.NoError(t, err)
		for epoch, partitions := range expectedDeadlineExpQueue {
			var bf abi.BitField
			found, err := expirationEpochs.Get(uint64(epoch), &bf)
			require.NoError(t, err)
			require.True(t, found)
			for _, p := range partitions {
				present, err := bf.IsSet(p)
				require.NoError(t, err)
				assert.True(t, present)
			}
		}
	}

	// Validate early terminations.
	assertBitfieldEquals(t, dl.EarlyTerminations, partitionsWithEarlyTerminations...)
}
