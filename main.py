import random


def main():
    bitmap_str: str = ""
    for i in range(1, 32767):
        bitmap_str += str(random.randint(a=0, b=1))
    random.random()
    print(bitmap_str)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
