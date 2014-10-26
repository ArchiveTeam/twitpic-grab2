'''Script to help generate item names.'''


def int_to_str(num, alphabet):
    '''Convert integer to string.'''
    # http://stackoverflow.com/a/1119769/1524507
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def main():
    start_num = 0
    end_num = 868128192
    pics_per_item = 100
    alphabet = '0123456789abcdefghijklmnopqrstuvwxyz'

    counter = start_num
    while True:
        lower = counter
        upper = min(counter + pics_per_item - 1, end_num)

        print('image:{0}:{1}'.format(
            int_to_str(lower, alphabet),
            int_to_str(upper, alphabet)
        ))

        counter += pics_per_item
        if counter > end_num:
            break


if __name__ == '__main__':
    main()
