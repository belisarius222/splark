def test_imports():
    from splark import Master  # NOQA
    from splark import RDD  # NOQA
    from splark import SplarkContext  # NOQA
    from splark import Worker  # NOQA


def test_SplarkContext():
    from splark import SplarkContext
    from splark.tests import DummyMaster

    dm = DummyMaster()
    with SplarkContext(master=dm) as sc:
        sc.parallelize(range(10))
        assert dm.num_workers > 0

    sc.stop()
    assert dm.num_workers == 0, "SplarkContext failed have workers killed on stop."


def test_RDD_basic():
    from splark import SplarkContext
    from splark.tests import DummyMaster

    with SplarkContext(master=DummyMaster()) as sc:
        rdd = sc.parallelize(range(10))
        collected = rdd.collect()
        assert collected == list(range(10)), collected


def test_RDD_map():
    from splark import SplarkContext
    from splark.tests import DummyMaster

    with SplarkContext(master=DummyMaster()) as sc:
        initial = list(range(10))
        rdd = sc.parallelize(initial)
        mappend = lambda x: x * x
        rdd2 = rdd.map(mappend)

        collected = rdd2.collect()
        assert collected == [mappend(x) for x in initial], collected


def test_RDD_reduce():
    from splark import SplarkContext
    from splark.tests import DummyMaster

    with SplarkContext(master=DummyMaster()) as sc:
        initial = list(range(10))
        rdd = sc.parallelize(initial)

        reduced = rdd.reduce(lambda x, y: x + y)
        assert reduced == sum(initial), reduced
