

if __name__ == '__main__':
    def digital_root(n):
        while n > 10:
            n = sum(int(digit) for digit in str(n))
        return n

    number = int(input('Enter a number: '))
    result = digital_root(number)
    print(f"the digital root of {number} is {result}")