def search_file(filename, pat):
    '''
    Searches for pattern in an ascii file
    '''
    return pat in peek_file(filename)


def peek_file(filename):
    '''
    Performs a simple file read followed by a file close
    '''
    with open(filename) as f:
        s = f.read()
    return s
