from collections import Counter


def oddEven(n):
    if n % 2 == 0:
        print("Even")
        return
    else:
        print("odd")
        return

if __name__ == '__main__':
    oddEven(11)
    countEven=0
    countOdd=0
    a= [1,2,3,4,56,78]
    res = map(lambda x: str(x) + " Even" if x % 2 == 0 else str(x) + " odd", a)
    print("\n".join(res))

    counts = Counter(['even' if num % 2 == 0 else 'odd' for num in a])

    # Printing the results
    print("Even numbers:", counts['even'])
    print("Odd numbers:", counts['odd'])


