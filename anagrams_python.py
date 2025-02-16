from collections import Counter

def anagrams(word1, word2):
    return Counter(word1) == Counter(word2)

if __name__ == "__main__":

    str1 = "listen"
    str2 = "silent"
    str3 = "hello"
    str4 = "elloh"
    print(sorted(str1))
    print(Counter(str1))
    print(Counter(str2))
    print(Counter(str3))
    print(Counter(str4))

    if sorted(str1) == sorted(str2):
        print("true sorted")
    else:
        print("false sorted")

    if Counter(str3) == Counter(str4):
        print("true count")
    else:
        print("false count")

    print(anagrams(str1, str2))