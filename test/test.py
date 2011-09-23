#!/usr/bin/env python2
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
        bears.insert(0,u'cartesian bear')

        for expected,actual in zip(bears,
                [u'cartesian bear',u'grizzly',u'white bear', u'pedo bear']):
            self.assertEqual(expected,actual)
        self.assertEqual(bears.pop(),u'pedo bear')
        self.assertEqual(bears.pop(0),u'cartesian bear')

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
        villains[u'Magneto'] = 'Max Eisenhardt'
        try:
            self.assertIn(u'riddler', villains)
        except AttributeError:
            self.assertTrue(u'riddler' in villains)

        self.assertEqual(villains.get(u'riddler'), u'Edward Nigma')

        self.assertEqual(len(villains.keys()), 2)
        for expected,actual_key in zip([u'Magneto',u'riddler'],
                sorted(villains.keys())):
            self.assertEqual(expected,actual_key)
        for expected,actual_value in zip([u'Edward Nigma',u'Max Eisenhardt'],
                sorted(villains.values())):
            self.assertEqual(expected,actual_value)


        villains.update({'Green Goblin':'Norman Osborn','riddler':'E. Nigma'})
        self.assertEqual(villains['Green Goblin'],'Norman Osborn')
        self.assertEqual(villains['riddler'],'E. Nigma')

        del villains[u'riddler']
        self.assertEqual(len(villains.keys()), 2)
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

