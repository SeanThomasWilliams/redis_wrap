import unittest

from redis_wrap import get_redis, get_list, get_hash, get_set


class TestBase(unittest.TestCase):
    def setUp(self):
        get_redis().delete(u'bears')
        get_redis().delete(u'villains')
        get_redis().delete(u'fishes')

class TestList(TestBase):
    def test_list(self):
        bears = get_list(u'bears')

        bears.append(u'grizzly')
        self.assertEqual(len(bears), 1)

        self.assertTrue(all(bear == u'grizzly' for bear in bears))

        try:
            self.assertIn(u'grizzly', bears)
        except AttributeError: # needs to be changed
            self.assertTrue(u'grizzly' in bears)

        bears.extend([u'white bear', u'pedo bear'])
        self.assertEqual(len(bears), 3)

        bears[1] = u'polar bear'
        self.assertEqual(bears[1],u'polar bear')

        bears.remove(u'grizzly')
        try:
            self.assertNotIn(u'grizzly', bears)
        except AttributeError: # needs to be changed
            self.assertTrue(u'grizzly' not in bears)

class TestHash(TestBase):
    def test_hash(self):
        villains = get_hash(u'villains')
        try:
            self.assertNotIn(u'riddler', villains)
        except AttributeError:
            self.assertTrue(u'riddler' not in villains)

        villains[u'riddler'] = 'Edward Nigma'
        try:
            self.assertIn(u'riddler', villains)
        except AttributeError:
            self.assertTrue(u'riddler' in villains)

        self.assertEqual(villains.get(u'riddler'), u'Edward Nigma')

        self.assertEqual(len(villains.keys()), 1)
        self.assertEqual(villains.values(), [u'Edward Nigma'])

        del villains[u'riddler']
        self.assertEqual(len(villains.keys()), 0)
        try:
            self.assertNotIn(u'riddler', villains)
        except AttributeError:
            self.assertTrue(u'riddler' not in villains)


class TestSet(TestBase):
    def test_set(self):
        fishes = get_set(u'fishes')
        try:
            self.assertNotIn(u'nemo', fishes)
        except AttributeError:
            self.assertTrue(u'nemo' not in fishes)

        fishes.add(u'nemo')
        try:
            self.assertIn(u'nemo', fishes)
        except AttributeError:
            self.assertTrue(u'nemo' in fishes)

        self.assertTrue(all(fish == u'nemo' for fish in fishes))


if __name__ == '__main__':
    unittest.main()

