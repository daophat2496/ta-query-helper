l = [1, 2, 1, 3]
l1 = filter(lambda ele : ele not in l, l)

print(list(set(l)))
print(l.sort())

def test(b) :
    b += 1
    print(id(b))

a = 5
print(id(a))
test(a)
print(a)
print(id(a))
