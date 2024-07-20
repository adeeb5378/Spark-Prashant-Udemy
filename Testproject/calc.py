def input():
    a = []
    n = int(input("Enter no elements "))
    for i in range(n):
        x = int(input("Enter next element "))
        a.append(x)
    print(a)
    a.sort()
    print(a)
