import time
import pytest
import os
import glob
import asyncio
from redis import asyncio as aioredis
from pathlib import Path
import boto3
import logging

from . import dfly_args
from .utility import DflySeeder, wait_available_async

BASIC_ARGS = {"dir": "{DRAGONFLY_TMP}/"}

SEEDER_ARGS = dict(keys=12_000, dbcount=5, multi_transaction_probability=0)


class SnapshotTestBase:
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    def get_main_file(self, pattern):
        def is_main(f):
            return "summary" in f if pattern.endswith("dfs") else True

        files = glob.glob(str(self.tmp_dir.absolute()) + "/" + pattern)
        possible_mains = list(filter(is_main, files))
        assert len(possible_mains) == 1, possible_mains
        return possible_mains[0]

    async def wait_for_save(self, pattern):
        while True:
            files = glob.glob(str(self.tmp_dir.absolute()) + "/" + pattern)
            if not len(files) == 0:
                break
            await asyncio.sleep(1)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-rdb-{{timestamp}}"})
class TestRdbSnapshot(SnapshotTestBase):
    """Test single file rdb snapshot"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE RDB")
        assert await async_client.flushall()
        await async_client.execute_command("DEBUG LOAD " + super().get_main_file("test-rdb-*.rdb"))

        assert await seeder.compare(start_capture, port=df_server.port)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-rdbexact.rdb", "nodf_snapshot_format": None})
class TestRdbSnapshotExactFilename(SnapshotTestBase):
    """Test single file rdb snapshot without a timestamp"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE RDB")
        assert await async_client.flushall()
        main_file = super().get_main_file("test-rdbexact.rdb")
        await async_client.execute_command("DEBUG LOAD " + main_file)

        assert await seeder.compare(start_capture, port=df_server.port)


@dfly_args({**BASIC_ARGS, "dbfilename": "test-dfs"})
class TestDflySnapshot(SnapshotTestBase):
    """Test multi file snapshot"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        # save + flush + load
        await async_client.execute_command("SAVE DF")
        assert await async_client.flushall()
        await async_client.execute_command(
            "DEBUG LOAD " + super().get_main_file("test-dfs-summary.dfs")
        )

        assert await seeder.compare(start_capture, port=df_server.port)


# We spawn instances manually, so reduce memory usage of default to minimum


@dfly_args({"proactor_threads": "1"})
class TestDflyAutoLoadSnapshot(SnapshotTestBase):
    """Test automatic loading of dump files on startup with timestamp"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    cases = [
        ("rdb", "test-autoload1-{{timestamp}}"),
        ("df", "test-autoload2-{{timestamp}}"),
        ("rdb", "test-autoload3-{{timestamp}}.rdb"),
        ("rdb", "test-autoload4"),
        ("df", "test-autoload5"),
        ("rdb", "test-autoload6.rdb"),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("save_type, dbfilename", cases)
    async def test_snapshot(self, df_local_factory, save_type, dbfilename):
        df_args = {"dbfilename": dbfilename, **BASIC_ARGS, "port": 1111}
        if save_type == "rdb":
            df_args["nodf_snapshot_format"] = None
        with df_local_factory.create(**df_args) as df_server:
            async with df_server.client() as client:
                await wait_available_async(client)
                await client.set("TEST", hash(dbfilename))
                await client.execute_command("SAVE " + save_type)

        with df_local_factory.create(**df_args) as df_server:
            async with df_server.client() as client:
                await wait_available_async(client)
                response = await client.get("TEST")
                assert response == str(hash(dbfilename))


# save every 1 minute
@dfly_args({**BASIC_ARGS, "dbfilename": "test-cron", "snapshot_cron": "* * * * *"})
class TestCronPeriodicSnapshot(SnapshotTestBase):
    """Test periodic snapshotting"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(
            port=df_server.port, keys=10, multi_transaction_probability=0
        )
        await seeder.run(target_deviation=0.5)

        await super().wait_for_save("test-cron-summary.dfs")

        assert super().get_main_file("test-cron-summary.dfs")


@dfly_args({**BASIC_ARGS, "dbfilename": "test-set-snapshot_cron"})
class TestSetsnapshot_cron(SnapshotTestBase):
    """Test set snapshot_cron flag"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(
            port=df_server.port, keys=10, multi_transaction_probability=0
        )
        await seeder.run(target_deviation=0.5)

        await async_client.execute_command("CONFIG", "SET", "snapshot_cron", "* * * * *")

        await super().wait_for_save("test-set-snapshot_cron-summary.dfs")

        assert super().get_main_file("test-set-snapshot_cron-summary.dfs")


@dfly_args(
    {**BASIC_ARGS, "dbfilename": "test-save-rename-command", "rename_command": "save=save-foo"}
)
class TestSnapshotShutdownWithRenameSave(SnapshotTestBase):
    """Test set snapshot_cron flag"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_server, df_seeder_factory):
        """Checks that on shutdown we save snapshot"""
        seeder = df_seeder_factory.create(port=df_server.port)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()
        a_client = aioredis.Redis(port=df_server.port)

        df_server.stop()
        df_server.start()

        a_client = aioredis.Redis(port=df_server.port)
        await wait_available_async(a_client)
        await a_client.connection_pool.disconnect()

        assert await seeder.compare(start_capture, port=df_server.port)


@dfly_args({**BASIC_ARGS})
class TestOnlyOneSaveAtATime(SnapshotTestBase):
    """Dragonfly does not allow simultaneous save operations, send 2 save operations and make sure one is rejected"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, async_client, df_server):
        await async_client.execute_command(
            "debug", "populate", "1000000", "askldjh", "1000", "RAND"
        )

        async def save():
            try:
                res = await async_client.execute_command("save", "rdb", "dump")
                return True
            except Exception as e:
                return False

        save_commnands = [asyncio.create_task(save()) for _ in range(2)]

        num_successes = 0
        for result in asyncio.as_completed(save_commnands):
            num_successes += await result

        assert num_successes == 1, "Only one SAVE must be successful"


@dfly_args({**BASIC_ARGS})
class TestPathEscapes(SnapshotTestBase):
    """Test that we don't allow path escapes. We just check that df_server.start()
    fails because we don't have a much better way to test that."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        super().setup(tmp_dir)

    @pytest.mark.asyncio
    async def test_snapshot(self, df_local_factory):
        df_server = df_local_factory.create(dbfilename="../../../../etc/passwd")
        try:
            df_server.start()
            assert False, "Server should not start correctly"
        except Exception as e:
            pass


@dfly_args({**BASIC_ARGS, "dbfilename": "test-shutdown"})
class TestDflySnapshotOnShutdown(SnapshotTestBase):
    """Test multi file snapshot"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    async def _get_info_memory_fields(self, client):
        res = await client.execute_command("INFO MEMORY")
        fields = {}
        for line in res.decode("ascii").splitlines():
            if line.startswith("#"):
                continue
            k, v = line.split(":")
            if k == "object_used_memory" or k.startswith("type_used_memory_"):
                fields.update({k: int(v)})
        return fields

    async def _delete_all_keys(self, client):
        # Delete all keys from all DBs
        for i in range(0, SEEDER_ARGS["dbcount"]):
            await client.select(i)
            while True:
                keys = await client.keys("*")
                if len(keys) == 0:
                    break
                await client.delete(*keys)

    @pytest.mark.asyncio
    async def test_memory_counters(self, df_seeder_factory, df_server):
        a_client = aioredis.Redis(port=df_server.port)

        memory_counters = await self._get_info_memory_fields(a_client)
        assert memory_counters == {"object_used_memory": 0}

        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        memory_counters = await self._get_info_memory_fields(a_client)
        assert all(value > 0 for value in memory_counters.values())

        await self._delete_all_keys(a_client)
        memory_counters = await self._get_info_memory_fields(a_client)
        assert memory_counters == {"object_used_memory": 0}

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, df_server):
        """Checks that:
        1. After reloading the snapshot file the data is the same
        2. Memory counters after loading from snapshot is similar to before creating a snapshot
        3. Memory counters after deleting all keys loaded by snapshot - this validates the memory
           counting when loading from snapshot."""
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()
        a_client = aioredis.Redis(port=df_server.port)
        memory_before = await self._get_info_memory_fields(a_client)

        df_server.stop()
        df_server.start()

        a_client = aioredis.Redis(port=df_server.port)
        await wait_available_async(a_client)
        await a_client.connection_pool.disconnect()

        assert await seeder.compare(start_capture, port=df_server.port)
        memory_after = await self._get_info_memory_fields(a_client)
        for counter, value in memory_before.items():
            # Unfortunately memory usage sometimes depends on order of insertion / deletion, so
            # it's usually not exactly the same. For the test to be stable we check that it's
            # at least 50% that of the original value.
            assert memory_after[counter] >= 0.5 * value

        await self._delete_all_keys(a_client)
        memory_empty = await self._get_info_memory_fields(a_client)
        assert memory_empty == {"object_used_memory": 0}


@dfly_args({**BASIC_ARGS, "dbfilename": "test-info-persistence"})
class TestDflyInfoPersistenceLoadingField(SnapshotTestBase):
    """Test is_loading field on INFO PERSISTENCE during snapshot loading"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    def extract_is_loading_field(self, res):
        matcher = b"loading:"
        start = res.find(matcher)
        pos = start + len(matcher)
        return chr(res[pos])

    @pytest.mark.asyncio
    async def test_snapshot(self, df_seeder_factory, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.05)
        a_client = aioredis.Redis(port=df_server.port)

        # Wait for snapshot to finish loading and try INFO PERSISTENCE
        await wait_available_async(a_client)
        res = await a_client.execute_command("INFO PERSISTENCE")
        assert "0" == self.extract_is_loading_field(res)

        await a_client.connection_pool.disconnect()


# If DRAGONFLY_S3_BUCKET is configured, AWS credentials must also be
# configured.
@pytest.mark.skipif(
    "DRAGONFLY_S3_BUCKET" not in os.environ, reason="AWS S3 snapshots bucket is not configured"
)
@dfly_args({"dir": "s3://{DRAGONFLY_S3_BUCKET}{DRAGONFLY_TMP}", "dbfilename": ""})
class TestS3Snapshot:
    """Test a snapshot using S3 storage"""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_dir: Path):
        self.tmp_dir = tmp_dir

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_snapshot(self, df_seeder_factory, async_client, df_server):
        seeder = df_seeder_factory.create(port=df_server.port, **SEEDER_ARGS)
        await seeder.run(target_deviation=0.1)

        start_capture = await seeder.capture()

        try:
            # save + flush + load
            await async_client.execute_command("SAVE DF snapshot")
            assert await async_client.flushall()
            await async_client.execute_command(
                "DEBUG LOAD "
                + os.environ["DRAGONFLY_S3_BUCKET"]
                + str(self.tmp_dir)
                + "/snapshot-summary.dfs"
            )

            assert await seeder.compare(start_capture, port=df_server.port)
        finally:
            self._delete_objects(
                os.environ["DRAGONFLY_S3_BUCKET"],
                str(self.tmp_dir)[1:],
            )

    def _delete_objects(self, bucket, prefix):
        client = boto3.client("s3")
        resp = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )
        keys = []
        for obj in resp["Contents"]:
            keys.append({"Key": obj["Key"]})
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": keys},
        )
