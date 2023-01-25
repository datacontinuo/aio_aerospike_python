import unittest


class Connect(unittest.TestCase):
    def setUp(self) -> None:
        self.l = [1,2,3]
        return super().setUp()
    def test1(self):
        self.l.append(5)
        print(self.l)
    def test2(self):
        self.l.append(6)
        print(self.l)
    def test4(self):
        self.l.append(7)
        print(self.l)

if __name__ == '__main__':
    unittest.main()
