
def list_and(*args):
    return [all(tuple) for tuple in zip(*args)]

def list_or(*args):
    return [any(tuple) for tuple in zip(*args)]