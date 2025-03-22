class GameTreeNode:
    def __init__(self, key, game):
        self.left = None
        self.right = None
        self.val = key
        self.game = game

def insert(root, key, game):
    if root is None:
        return GameTreeNode(key, game)
    
    if key < root.val:
        root.left = insert(root.left, key, game)
    else:
        root.right = insert(root.right, key, game)
    
    return root

def inorder_traversal(root, res):
    if root:
        if root.left: inorder_traversal(root.left, res)
        res.append(root.game)
        if root.right: inorder_traversal(root.right, res)

def tree_sort(arr):
    if not arr:
        return arr
    
    root = None
    for e in arr:
        root = insert(root, e['score'], e)
    
    print("Tree sorted!")
    
    sorted_arr = []
    inorder_traversal(root, sorted_arr)
    print("BST sorted!")
    return sorted_arr
