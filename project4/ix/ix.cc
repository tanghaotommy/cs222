
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    RC rc = PagedFileManager::instance()->openFile(fileName, ixfileHandle.fileHandle);
    // printf("return value of openFile%d\n", rc);
    if (rc == 0)
        ixfileHandle.fileHandle.collectCounterValues(ixfileHandle.ixReadPageCounter, 
            ixfileHandle.ixWritePageCounter, ixfileHandle.ixAppendPageCounter);
    // printf("%d\n", ixfileHandle.fileHandle.getNumberOfPages());
    // printf("THERE\n");
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
#ifdef DEBUG_IX
    cout<<"[insertEntry]"<<endl;
    cout<<"-------before insert--------"<<endl;
    this-> printBtree(ixfileHandle,attribute);
#endif
    //cout<<"[insert entry] num of pages "<<ixfileHandle.fileHandle.getNumberOfPages()<<endl;
    if (ixfileHandle.fileHandle.getNumberOfPages() == 0)
    {
        Node root = Node(attribute);
        root.nodeType = RootOnly;
        root.appendKey(key);
        vector <RID> records;
        records.push_back(rid);
        root.pointers.push_back(records);
        // root.printRids();
        // printf("\n");
        root.cPage = 0;
        void *page = malloc(PAGE_SIZE);
        root.serialize(page);
        ixfileHandle.fileHandle.appendPage(page);
        free(page);
    } 
    else
    {
        
        void *page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.readPage(0, page);
        Node root = Node(&attribute, page, &ixfileHandle);
        // free(page);
        if (root.nodeType == RootOnly)
        {
            
            int pos = root.getChildPos(key);
            //printNode(ixfileHandle, attribute, 0);
            int isNewKey = root.insertKey(pos, key); //return 0 if the key already exit.
            root.insertPointer(pos, rid, key); //Insert into vector.
            vector<Node*> path;
            path.push_back(&root);

            if ((isNewKey || root.keys.size() > 1) && root.isFull())
            {
                split(path, ixfileHandle);                  
            } 
            else
            {   
                root.writeNodeToPage(ixfileHandle);	
            }

            for(int i=1;i<path.size();i++)
            {
                delete path[i];
            }
            path.clear();   
            free(page);    
        } 
        else
        {
            vector<Node*> path;
            traverseToLeafWithPath(ixfileHandle, &root, path, key, attribute);
            
            int pos = path[path.size() - 1]->getChildPos(key);
            int isNewKey = path[path.size() - 1]->insertKey(pos, key); //return 0 if the key already exit.
            path[path.size() - 1]->insertPointer(pos, rid, key); //Insert into vector.
#ifdef DEBUG_IX              
            printf("[Split] isFull: %d, isNewKey: %d\n", path[path.size() - 1]->isFull(), isNewKey);
#endif  
            if((isNewKey || path[path.size() - 1]->keys.size() > 1) && path[path.size() - 1]->isFull())
            {
                split(path, ixfileHandle);
            }
            else 
            {
                path[path.size() - 1]->writeNodeToPage(ixfileHandle);
                for(int i=1;i<path.size();i++)
                {
                    delete path[i];
                }
                path.clear();
            }
            
            free(page);
        }
    }
#ifdef DEBUG_IX
    cout<<"--------After insert--------"<<endl;
    this-> printBtree(ixfileHandle,attribute);
#endif

return 0;
}

RC IndexManager::traverseToLeafWithPath(IXFileHandle &ixfileHandle, Node *root, vector<Node*> &path, const void *key, const Attribute &attribute)
{
    path.push_back(root);
    //path.push_back(&root);
    //cout<<"[TraverseToLeafWithPath] "<<path.size()<<endl;
    if(root->nodeType == LeafNode) return 0;
    int pos = root->getChildPos(key);            
    void *page = malloc(PAGE_SIZE);     
    ixfileHandle.fileHandle.readPage(root->children[pos], page);
    Node *node = new Node(&attribute, page, &ixfileHandle);   
    node->cPage = root->children[pos];
    free(page);   
    traverseToLeafWithPath(ixfileHandle, node, path, key, attribute);                
    return 0;
}

RC IndexManager::split(vector<Node*> path, IXFileHandle &ixfileHandle)
{
    Node *node = path[path.size() - 1];    
    if(node->nodeType == LeafNode)
    {
        int order = node->keys.size() / 2; 

        Node new_leaf = Node(node->attribute);
        new_leaf.nodeType = LeafNode;
	    new_leaf.overFlowPages = node->overFlowPages;

        for(int i=order;i < node->keys.size();i++)
        {
            new_leaf.appendKey(node->keys[i]);
            new_leaf.appendPointer(node->pointers[i]);
        }
        node->keys.erase(node->keys.begin() + order,node->keys.begin() + node->keys.size());
        node->pointers.erase(node->pointers.begin() + order,node->pointers.begin() + node->pointers.size());
        void *page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.appendPage(page);
        new_leaf.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        free(page);
        new_leaf.next = node->next;
        node->next = new_leaf.cPage;
        node->writeNodeToPage(ixfileHandle);
        new_leaf.previous = node->cPage;
        new_leaf.writeNodeToPage(ixfileHandle);

        Node *parent = path[path.size() - 2];

        int pos = parent->getChildPos(new_leaf.keys[0]);
        int isNewKey = parent->insertKey(pos, new_leaf.keys[0]); 

        parent->insertChild(pos + 1, new_leaf.cPage); 
        
        delete path[path.size() - 1];
        path.pop_back();

        if((isNewKey || parent->keys.size() > 1) && parent->isFull())
        {
            split(path, ixfileHandle);            
        }
        else
        {
            parent->writeNodeToPage(ixfileHandle);
        }
    }
    else if(node->nodeType == InternalNode)
    {
        int order = node->keys.size() / 2; 
        Node new_intermediate(node->attribute);
        
        Node *parent = path[path.size() - 2];

        new_intermediate.nodeType = node->nodeType;

        for(int i = order + 1;i < node->keys.size();i++)
        {
            new_intermediate.appendKey(node->keys[i]);
            new_intermediate.appendChild(node->children[i]);
        }
       new_intermediate.appendChild(node->children[node->children.size() - 1]);

        void *page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.appendPage(page);
        new_intermediate.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        free(page);
        int pos = parent->getChildPos(node->keys[order]);
        int isNewKey = parent->insertKey(pos, node->keys[order]); 
        parent->insertChild(pos + 1, new_intermediate.cPage);  

        new_intermediate.writeNodeToPage(ixfileHandle);
        node->keys.erase(node->keys.begin() + order,node->keys.begin() + node->keys.size());
        node->children.erase(node->children.begin() + order + 1,node->children.begin() + node->children.size());
        node->writeNodeToPage(ixfileHandle);

        delete path[path.size() - 1];
        path.pop_back();
        if((isNewKey || parent->keys.size() > 1) && parent->isFull())
        {
            split(path, ixfileHandle);            
        }
        else 
        {
            parent->writeNodeToPage(ixfileHandle);

        }
    }
    else if(node->nodeType == RootOnly)
    {
        int order = node->keys.size() / 2; 
        Node new_leafNode1(node->attribute);
        Node new_leafNode2(node->attribute);
        node->nodeType = RootNode;
        new_leafNode1.nodeType = LeafNode;
        new_leafNode2.nodeType = LeafNode;
	    new_leafNode1.overFlowPages = node->overFlowPages;
	    new_leafNode2.overFlowPages = node->overFlowPages;
        //node->children.append();
        for(int i = 0;i < node->keys.size();i++)
        {
            if(i < order){
                new_leafNode1.appendKey(node->keys[i]);
                new_leafNode1.appendPointer(node->pointers[i]);
            }
            else if(i >= order){   //
                new_leafNode2.appendKey(node->keys[i]);
                new_leafNode2.appendPointer(node->pointers[i]);
            }
        }
        node->keys.erase(node->keys.begin() + order+1,node->keys.begin() + node->keys.size());
        node->keys.erase(node->keys.begin(),node->keys.begin() + order);
        node->pointers.clear();
        void *page1 = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.appendPage(page1);
        new_leafNode1.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        void *page2 = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.appendPage(page2);
        new_leafNode2.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        free(page1);
        free(page2);
        node->children.push_back(new_leafNode1.cPage);
        node->children.push_back(new_leafNode2.cPage);
        node->writeNodeToPage(ixfileHandle);

        new_leafNode1.next = new_leafNode2.cPage;
        new_leafNode2.previous = new_leafNode1.cPage;
        new_leafNode1.writeNodeToPage(ixfileHandle);
        new_leafNode2.writeNodeToPage(ixfileHandle);

    }
    else if(node->nodeType == RootNode)
    {
        int order = node->keys.size() / 2;  
        Node new_intermediate1(node->attribute);
        Node new_intermediate2(node->attribute);

        new_intermediate1.nodeType = InternalNode;
        new_intermediate2.nodeType = InternalNode;
        for(int i = 0;i < node->keys.size();i++)
        {
            if(i < order){
                new_intermediate1.appendKey(node->keys[i]);
            }
            else if(i > order){
                new_intermediate2.appendKey(node->keys[i]);
            }
        }
        for(int i = 0;i < node->children.size();i++)
        {
            if(i < order + 1){
                new_intermediate1.appendChild(node->children[i]);
            }
            else if(i >= order + 1){
                new_intermediate2.appendChild(node->children[i]);
            }
        }
        node->keys.erase(node->keys.begin() + order+1,node->keys.begin() + node->keys.size());
        node->keys.erase(node->keys.begin(),node->keys.begin() + order);
        void *page1 = malloc(PAGE_SIZE);
        void *page2 = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.appendPage(page1);
        new_intermediate1.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        ixfileHandle.fileHandle.appendPage(page2);
        new_intermediate2.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        free(page1);
        free(page2);
        node->children.clear();        
        node->children.push_back(new_intermediate1.cPage);
        node->children.push_back(new_intermediate2.cPage);
        node->writeNodeToPage(ixfileHandle);

        new_intermediate1.writeNodeToPage(ixfileHandle);
        new_intermediate2.writeNodeToPage(ixfileHandle);
    }
    return 0;
}

RC Node::writeNodeToPage(IXFileHandle &ixfileHandle){
    if ((this->nodeType == RootOnly || this->nodeType == LeafNode) && this->keys.size() == 1 && this->getNodeSize() > PAGE_SIZE)
    {
        int sizeofRid = 2 * sizeof(int);
        int nRidsInNode = (PAGE_SIZE - this->getHeaderAndKeysSize()) / sizeofRid;
        int left = this->pointers[0].size() - nRidsInNode;
        int nRidsPerPage = (PAGE_SIZE - 2*sizeof(int)) / sizeofRid;
        int needed = left / nRidsPerPage + 1;
#ifdef DEBUG_IX
	printf("[writeNodeToPage] size of node: %d, size of overhead: %d\n", this->size, this->getHeaderAndKeysSize());
	printf("[writeNodeToPage] Overflow, size of node: %d, page needed: %d, nRidsPerPage: %d, total # keys: %d, nRidsInNode: %d\n",
		this->size, needed, nRidsPerPage, this->pointers[0].size(), nRidsInNode);
#endif
        if (needed > this->overFlowPages.size())
        {
            void *page = malloc(PAGE_SIZE);
            ixfileHandle.fileHandle.appendPage(page);
            this->overFlowPages.push_back(ixfileHandle.fileHandle.getNumberOfPages() - 1);
        }

        //serialize original node
        void *page = malloc(PAGE_SIZE);
        int offset = 0;
        memcpy((char *)page + offset, &this->nodeType, sizeof(NodeType));
        offset += sizeof(NodeType);
        memcpy((char *)page + offset, &this->previous, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)page + offset, &this->next, sizeof(int));
        offset += sizeof(int);

        int nKeys = this->keys.size();
        memcpy((char *)page + offset, &nKeys, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nKeys; ++i)
        {
            if (this->attrType == TypeInt)
            {
                memcpy((char *)page + offset, this->keys[i], sizeof(attribute->length));
                offset += sizeof(attribute->length);
            }
            else if (this->attrType == TypeReal)
            {
                memcpy((char *)page + offset, this->keys[i], sizeof(attribute->length));
                offset += sizeof(attribute->length);
            }
            else if (this->attrType == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, this->keys[i], sizeof(int));
                memcpy((char *)page + offset, &nameLength, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                memcpy((char *)page + offset, (char *) this->keys[i] + sizeof(int), nameLength);
                offset += nameLength;
            }
        }

        int nRids = this->pointers.size();
        memcpy((char *)page + offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords = nRidsInNode;
            memcpy((char *)page + offset, &nRecords, sizeof(int));
            offset += sizeof(int);

            for (int j = 0; j < nRecords; ++j)
            {
                int pageNum = this->pointers[i][j].pageNum;
                int slotNum = this->pointers[i][j].slotNum;
                memcpy((char *)page + offset, &pageNum, sizeof(int));
                offset += sizeof(int);
                memcpy((char *)page + offset, &slotNum, sizeof(int));
                offset += sizeof(int);
            }
        }
        memcpy((char *)page + offset, &this->overFlowPages[0], sizeof(int));
        offset += sizeof(int);
#ifdef DEBUG_IX
        printf("[writePageToNode] Original node offset: %d\n", offset);
	printf("[writePageToNode] Original page contains Rids from %d to %d\n", 0, nRidsInNode - 1);
#endif
        ixfileHandle.fileHandle.writePage(this->cPage, page);

        //serialize each overflow pages
#ifdef DEBUG_IX
	printf("[");
	for (int i = 0; i < this->overFlowPages.size(); ++i)
	{
	    printf("%d, ", this->overFlowPages[i]);
	}
	printf("]\n");
#endif
        for (int i = 0; i < this->overFlowPages.size(); ++i)
        {
            if (i != this->overFlowPages.size() - 1)
            {
                int offset = this->serializeOverflowPage(i*nRidsPerPage + nRidsInNode, (i+1)*nRidsPerPage + nRidsInNode, page);
                memcpy((char *)page + offset, &this->overFlowPages[i + 1], sizeof(int));
            }
            else
            {
                int offset = this->serializeOverflowPage(i*nRidsPerPage + nRidsInNode, this->pointers[0].size(), page);
#ifdef DEBUG_IX
        printf("[writePageToNode] Last overflow node offset: %d\n", offset);
#endif
                int nextPage = -1;
                memcpy((char *)page + offset, &nextPage, sizeof(int));
            }
            ixfileHandle.fileHandle.writePage(this->overFlowPages[i], page);
        }
        free(page);
        return 0;
    }
    else
    {
#ifdef DEBUG_IX
	printf("[writeNodeToPage] Normal page write\n");
#endif
        void *page = malloc(PAGE_SIZE);
        this->serialize(page);
        ixfileHandle.fileHandle.writePage(this->cPage, page);
        free(page);
    }
    return 0;
}

int Node::insertKey(int pos, const void* key)
{
#ifdef DEBUG_IX
    printf("[insertKey]inserting keys, type: %d, pos : %d, total number of keys: %d, size of node: %d\n", 
    this->attrType, pos, this->keys.size(), this->getNodeSize());
#endif
    if (this->attrType == TypeInt)
    {
        // printf("[insertKey] inserting keys int\n");
        void *data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        int d;
        memcpy(&d, (char *)key, sizeof(attribute->length));
        if(pos>=1)
            bool x = isEqual(this->keys[pos - 1], data, this->attribute);
        if(this->keys.size() >= 1  && pos >= 1 && pos >= 1 && isEqual(this->keys[pos - 1], data, this->attribute))
        {
            free(data);
            return 0;
        } 
        this->keys.insert(this->keys.begin() + pos, data);
        // this->size += sizeof(attribute->length);
    }
    else if (this->attrType == TypeReal)
    {
        void * data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        if(this->keys.size() >= 1  && pos >= 1 && isEqual(this->keys[pos - 1], data, this->attribute))         
        {
            free(data);
            return 0;
        } 
        this->keys.insert(this->keys.begin() + pos, data);
        // this->size += sizeof(attribute->length);
    }
    else if (this->attrType == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)key, sizeof(int));
        // printf("String length: %d\n", nameLength);
        void* data = malloc(nameLength + sizeof(int));
        memcpy(data, &nameLength, sizeof(int));
        memcpy((char *) data + sizeof(int), (char *)key + sizeof(int), nameLength);
#ifdef DEBUG_IX
        printf("[insertKey]Length of the key: %d\n", nameLength);
#endif
        if(this->keys.size() >= 1  && pos >= 1 && isEqual(this->keys[pos - 1], data, this->attribute))  
        {
            free(data);
            return 0;
        } 
        this->keys.insert(this->keys.begin() + pos, data);     
        // this->size += sizeof(int) + nameLength; 
    }
    return 1;
}

RC Node::insertPointer(int pos, const RID &rid, const void* key)
{
#ifdef DEBUG_IX
    printf("[insertPointer] InsertPointer to %d, total keys %d\n", pos, this->keys.size());
#endif
    RID data_rid;
    int data_rid_pageNum;
    int data_rid_slotNum;
    memcpy(&data_rid_pageNum, &rid.pageNum, sizeof(int));
    memcpy(&data_rid_slotNum, &rid.slotNum, sizeof(int));
    data_rid.pageNum = data_rid_pageNum;
    data_rid.slotNum = data_rid_slotNum;

    if(this->keys.size() >= 1 && pos >= 1 && isEqual(this->keys[pos - 1], key, this->attribute)){
        this->pointers[pos - 1].push_back(data_rid);
    }
    else{
        vector<RID> vector_rid;
        vector_rid.push_back(data_rid);
        this->pointers.insert(this->pointers.begin() + pos, vector_rid);
    }
#ifdef DEBUG_IX
    printf("[insertPointer]");
    this->printKeys();
    printf(" ");
    this->printRids(0);
#endif
    return 0;
}

RC Node::insertChild(int pos, int pageNum)
{
    int data;
    memcpy(&data,&pageNum, sizeof(int));
    this->children.insert(this->children.begin() + pos, data);
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
/*
    cout<<"[DeleteEntry]"<<endl;
    cout<<"-----------Before delete--------------"<<endl;
    printBtree(ixfileHandle, attribute);
    */

    if (ixfileHandle.fileHandle.getNumberOfPages() == 0) return -1;
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(0, page);
    Node root = Node(&attribute, page, &ixfileHandle); 
    int pos;   
    int ridIsSingle;
    free(page);
    if(root.nodeType == RootOnly)
    {
        pos = root.findKey(key);
        if(pos < 0) return -1;
        ridIsSingle = root.deleteRecord(pos, rid);
        if(ridIsSingle == 1)
        {
            root.keys.erase(root.keys.begin() + pos, root.keys.begin() + pos + 1);
            
        }
        else if(ridIsSingle == -1) return -1;
        root.writeNodeToPage(ixfileHandle);

        return 0;
    }
    vector<Node*> path;
    traverseToLeafWithPath(ixfileHandle, &root, path, key, attribute);
    Node *leaf = path[path.size()-1];

    pos = leaf->findKey(key);

    if(pos < 0){
        return -1;
    }
    ridIsSingle = leaf->deleteRecord(pos, rid);
    if(ridIsSingle == 1)
    {
        leaf->keys.erase(leaf->keys.begin() + pos, leaf->keys.begin() + pos + 1);
    }
    else if(ridIsSingle == -1){
        return -1;
    }
    

    merge(path, ixfileHandle);
    
    // for(int i=1;i<path.size();i++)
    // {
    //     delete path[i];
    // }
    // path.clear();

    return 0;
}

RC IndexManager::merge(vector<Node*> path, IXFileHandle &ixfileHandle)
{
    Node *node = path[path.size() - 1];
    Node *parent = path[path.size() - 2];

    int sibling;
    int position = -1;
    int direction = 0; //0:right, 1:left
    if(parent->children[parent->children.size() - 1] != node->cPage)
    {
        sibling = node->getRightSibling(ixfileHandle, parent, position);
        direction = 0;
    }
    else{
        sibling = node->getLeftSibling(ixfileHandle, parent, position);
        direction = 1;
    }
    if(sibling == -1 || position == -1) return -1;

    void *page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(sibling, page);
    Node siblingNode = Node(node->attribute, page, &ixfileHandle); 
    siblingNode.cPage = sibling;
    free(page);
    // cout<<"-----------------node pagesize----------"<<node->getNodeSize()<<endl;
    // cout<<"-----------------sibling pagesize----------"<<siblingNode.getNodeSize()<<" isLesshalffull: "<<siblingNode.isLessHalfFull()<<endl;
    if(node->isLessHalfFull() && siblingNode.isLessHalfFull() && node->getNodeSize() + siblingNode.getNodeSize() + 4 < PAGE_SIZE)
    {
        doMerge(ixfileHandle, &siblingNode, path, position, direction);
    }
    else if(node->isLessHalfFull())
    {
         borrow(ixfileHandle, node, &siblingNode, parent, position, direction);
        for(int i=1;i<path.size();i++)
        {
            delete path[i];
        }
        path.clear();
    }
    else
    {
        node->writeNodeToPage(ixfileHandle);
        for(int i=1;i<path.size();i++)
        {
            delete path[i];
        }
        path.clear();
    }

    return 0;
}

RC IndexManager::doMerge(IXFileHandle &ixfileHandle, Node *nextNode, vector<Node*> path, int &pos, int &direction)
{
    Node *node = path[path.size() - 1];
   
    if(direction == 0)
    {
        if(node->nodeType == LeafNode)
        {

                Node *parent = path[path.size() - 2];
                if(parent->nodeType == RootNode && parent->keys.size() == 1) 
                {
                    node->nodeType = RootOnly; 
                    node->cPage = 0;
                    parent->cPage = -1;
                }

                for(int i=0;i<nextNode->keys.size();i++)
                {
                    node->appendKey(nextNode->keys[i]);
                    node->appendPointer(nextNode->pointers[i]);
                }
                
                parent->keys.erase(parent->keys.begin() + pos, parent->keys.begin() + pos + 1);
                parent->children.erase(parent->children.begin() + pos + 1, parent->children.begin() + pos + 2);
                node->next = nextNode->next;
                if(node->nodeType == LeafNode && nextNode->next != -1)
                {
                    void *page2 = malloc(PAGE_SIZE);
                    ixfileHandle.fileHandle.readPage(node->next, page2);
                    Node nextNextChild = Node(node->attribute, page2, &ixfileHandle); 
                    free(page2);
                    nextNextChild.previous = node->cPage;
                    nextNextChild.writeNodeToPage(ixfileHandle);
                }

                parent->writeNodeToPage(ixfileHandle);
                node->writeNodeToPage(ixfileHandle);

                delete path[path.size()-1];
                path.pop_back();
                if(path.size() == 1){
                    return 0;   //do not need to check whether its parent need to merge with parent's sibling 
                                //because its parents is rootNode
                } 

                Node *grandParent = path[path.size() - 2];

                int sibling;
                int position = -1;
                if(grandParent->children[grandParent->children.size() - 1] != parent->cPage)
                {
                    sibling = parent->getRightSibling(ixfileHandle, grandParent, position);
                    direction = 0;
                }
                else{
                    sibling = parent->getLeftSibling(ixfileHandle, grandParent, position);
                    direction = 1;
                }
                if(sibling == -1 || position == -1) return -1;
                void *page = malloc(PAGE_SIZE);
                ixfileHandle.fileHandle.readPage(sibling, page);
                Node parentSiblingNode = Node(parent->attribute, page, &ixfileHandle); 
                parentSiblingNode.cPage = sibling;
                free(page);

                if(parent->isLessHalfFull() && parentSiblingNode.isLessHalfFull() && parent->getNodeSize() + parentSiblingNode.getNodeSize() + 4 < PAGE_SIZE)
                {
                    doMerge(ixfileHandle, &parentSiblingNode, path, position, direction);
                }
                else if(parent->isLessHalfFull())
                {
                    borrow(ixfileHandle, parent, &parentSiblingNode, grandParent, position, direction);
                }
                else{
                    return 0;
                }
        }
        else if(node->nodeType == InternalNode)
        {
                Node *parent = path[path.size() - 2];
                if(parent->nodeType == RootNode && parent->keys.size() == 1)
                {
                    node->nodeType = RootNode; 
                    node->cPage = 0;
                    parent->cPage = -1;
                }
                    
                if(parent)
                node->appendKey(parent->keys[pos]);
                for(int i=0;i<nextNode->keys.size();i++)
                {
                    node->appendKey(nextNode->keys[i]);
                    node->appendChild(nextNode->children[i]);
                }
                node->appendChild(nextNode->children[nextNode->children.size() - 1]);

                parent->keys.erase(parent->keys.begin() + pos, parent->keys.begin() + pos + 1);
                parent->children.erase(parent->children.begin() + pos + 1, parent->children.begin() + pos + 2);
                parent->writeNodeToPage(ixfileHandle);
                node->writeNodeToPage(ixfileHandle);

                delete path[path.size()-1];
                path.pop_back();

                if(path.size() == 1){
                    return 0;  //do not need to check whether its parent need to merge with parent's sibling 
                                                //because its parents is rootNode
                } 
                Node *grandParent = path[path.size() - 2]; //check whether its parent need to merge with parent's sibling
                int sibling;
                int position = -1;
                if(grandParent->children[grandParent->children.size() - 1] != parent->cPage)
                {
                    sibling = parent->getRightSibling(ixfileHandle, grandParent, position);
                    direction = 0;
                }
                else{
                    sibling = parent->getLeftSibling(ixfileHandle, grandParent, position);
                    direction = 1;
                }
                if(sibling == -1 || position == -1) return -1;
                void *page = malloc(PAGE_SIZE);
                ixfileHandle.fileHandle.readPage(sibling, page);
                Node parentSiblingNode = Node(parent->attribute, page, &ixfileHandle); 
                parentSiblingNode.cPage = sibling;
                free(page);

                if(parent->isLessHalfFull() && parentSiblingNode.isLessHalfFull() && parent->getNodeSize() + parentSiblingNode.getNodeSize() + 4 < PAGE_SIZE)
                {
                    doMerge(ixfileHandle, &parentSiblingNode, path, position, direction);
                }
                else if(parent->isLessHalfFull())
                {
                    borrow(ixfileHandle, parent, &parentSiblingNode, grandParent, position, direction);
                }
                else{
                    return 0;
                }
        }
    }
    else if(direction == 1)
    {
        if(node->nodeType == LeafNode)
        {
            pos = pos - 1;
            Node *parent = path[path.size() - 2];
            if(parent->nodeType == RootNode && parent->keys.size() == 1) 
            { //here, nextNode is the left sibling node
                nextNode->nodeType = RootOnly; 
                nextNode->cPage = 0;
                parent->cPage = -1;
            }  
            for(int i=0;i<node->keys.size();i++)
            {
                nextNode->appendKey(node->keys[i]);
                nextNode->appendPointer(node->pointers[i]);
            }
            
            parent->keys.erase(parent->keys.begin() + pos, parent->keys.begin() + pos + 1);
            parent->children.erase(parent->children.begin() + pos + 1, parent->children.begin() + pos + 2);
            // parent->printChildren();
            // cout<<"+++++++++parent children size: "<<parent->children.size()<<endl;
            
            if(node->nodeType == LeafNode)
                nextNode->next = node->next;
            parent->writeNodeToPage(ixfileHandle);
            nextNode->writeNodeToPage(ixfileHandle);
            delete path[path.size()-1];
            path.pop_back();

            if(path.size() == 1) {
                return 0;  //do not need to check whether its parent need to merge with parent's sibling 
                                            //because its parents is rootNode
            }
            Node *grandParent = path[path.size() - 2];

            int sibling;
            int position = -1;
            if(grandParent->children[grandParent->children.size() - 1] != parent->cPage)
            {
                sibling = parent->getRightSibling(ixfileHandle, grandParent, position);
                direction = 0;
            }
            else{
                sibling = parent->getLeftSibling(ixfileHandle, grandParent, position);
                direction = 1;
            }
            if(sibling == -1 || position == -1) return -1;
            void *page = malloc(PAGE_SIZE);
            ixfileHandle.fileHandle.readPage(sibling, page);
            Node parentSiblingNode = Node(parent->attribute, page, &ixfileHandle); 
            parentSiblingNode.cPage = sibling;
            free(page);

            if(parent->isLessHalfFull() && parentSiblingNode.isLessHalfFull() && parent->getNodeSize() + parentSiblingNode.getNodeSize() + 4 < PAGE_SIZE)
            {
                doMerge(ixfileHandle, &parentSiblingNode, path, position, direction);
            }
            else if(parent->isLessHalfFull())
            {
                borrow(ixfileHandle, parent, &parentSiblingNode, grandParent, position, direction);
            }
            else{
                return 0;
            }
        }
        else if(node->nodeType == InternalNode)
        {
            pos = pos - 1;
            Node *parent = path[path.size() - 2];
            if(parent->nodeType == RootNode && parent->keys.size() == 1)
            {
                nextNode->nodeType = RootNode; 
                nextNode->cPage = 0;
                parent->cPage = -1;
            }
                
            nextNode->appendKey(parent->keys[pos]);
            for(int i=0;i<node->keys.size();i++)
            {
                nextNode->appendKey(node->keys[i]);
                nextNode->appendChild(node->children[i]);
            }
            nextNode->appendChild(node->children[node->children.size() - 1]);

            parent->keys.erase(parent->keys.begin() + pos, parent->keys.begin() + pos + 1);
            parent->children.erase(parent->children.begin() + pos + 1, parent->children.begin() + pos + 2);
            parent->writeNodeToPage(ixfileHandle);
            nextNode->writeNodeToPage(ixfileHandle);

            delete path[path.size()-1];
            path.pop_back();

            if(path.size() == 1){
                return 0;  //do not need to check whether its parent need to merge with parent's sibling 
                                            //because its parents is rootNode
            }
            Node *grandParent = path[path.size() - 2]; //check whether its parent need to merge with parent's sibling
            int sibling;
            int position = -1;
            if(grandParent->children[grandParent->children.size() - 1] != parent->cPage)
            {
                sibling = parent->getRightSibling(ixfileHandle, grandParent, position);
                direction = 0;
            }
            else{
                sibling = parent->getLeftSibling(ixfileHandle, grandParent, position);
                direction = 1;
            }
            if(sibling == -1 || position == -1) return -1;
            void *page = malloc(PAGE_SIZE);
            ixfileHandle.fileHandle.readPage(sibling, page);
            Node parentSiblingNode = Node(parent->attribute, page, &ixfileHandle); 
            parentSiblingNode.cPage = sibling;
            free(page);

            if(parent->isLessHalfFull() && parentSiblingNode.isLessHalfFull() && parent->getNodeSize() + parentSiblingNode.getNodeSize() + 4 < PAGE_SIZE)
            {
                doMerge(ixfileHandle, &parentSiblingNode, path, position, direction);
            }
            else if(parent->isLessHalfFull())
            {
                borrow(ixfileHandle, parent, &parentSiblingNode, grandParent, position, direction);
            }
            else{
                return 0;
            }
        }
    }
    return 0;
}

RC IndexManager::borrow(IXFileHandle &ixfileHandle, Node* node, Node *sibling, Node *parent, int &position, int &direction)
{
    if(sibling->keys.size() <= 1) return 0;

    if(direction == 0)
    {
        if(node->nodeType == LeafNode)
        {
            if(node->getNodeSize() + getKeySize(sibling->keys[0], sibling->attribute) + getRidSize(sibling->pointers[0]) > PAGE_SIZE) //can't borrow
            {
                return 0;
            }
            node->appendKey(sibling->keys[0]);
            node->appendPointer(sibling->pointers[0]);
            sibling->keys.erase(sibling->keys.begin(), sibling->keys.begin() + 1);
            sibling->pointers.erase(sibling->pointers.begin(), sibling->pointers.begin() + 1);

            parent->replaceKey(position, sibling->keys[0]);
            
            node->writeNodeToPage(ixfileHandle);
            sibling->writeNodeToPage(ixfileHandle);
            parent->writeNodeToPage(ixfileHandle);
           
        }
        else if(node->nodeType == InternalNode)
        {
            if(node->getNodeSize() + getKeySize(sibling->keys[0], sibling->attribute) + 4 > PAGE_SIZE) //can't burrow
            {
                return 0;
            }
            node->appendKey(parent->keys[position]);
            node->appendChild(sibling->children[0]);
            parent->replaceKey(position, sibling->keys[0]);
            sibling->keys.erase(sibling->keys.begin(), sibling->keys.begin() + 1);
            sibling->children.erase(sibling->children.begin(), sibling->children.begin() + 1);
           

            node->writeNodeToPage(ixfileHandle);
            sibling->writeNodeToPage(ixfileHandle);
            parent->writeNodeToPage(ixfileHandle);
        }
    }
    else if(direction == 1)
    {
        if(node->nodeType == LeafNode)
        {
            int keysSize = sibling->keys.size();
            int pointerSize = sibling->pointers.size();
            
            if(node->getNodeSize() + getKeySize(sibling->keys[keysSize - 1], sibling->attribute) + getRidSize(sibling->pointers[pointerSize - 1]) > PAGE_SIZE) //can't borrow
            {
                return 0;
            }
            
            for(int i=0;i<sibling->pointers[pointerSize - 1].size();i++)
            {
                node->insertPointer(0, sibling->pointers[pointerSize - 1][i], sibling->keys[keysSize - 1]);
            }
            node->insertKey(0,sibling->keys[keysSize - 1]);
            sibling->keys.erase(sibling->keys.begin() + keysSize - 1, sibling->keys.begin() + keysSize);
            sibling->pointers.erase(sibling->pointers.begin() + pointerSize - 1, sibling->pointers.begin() + pointerSize);
            parent->replaceKey(position - 1, sibling->keys[0]); //because child are 1 bigger than its left key

            node->writeNodeToPage(ixfileHandle);
            sibling->writeNodeToPage(ixfileHandle);
            parent->writeNodeToPage(ixfileHandle);
        }
        else if(node->nodeType == InternalNode)
        {
            int keysSize = sibling->keys.size();
            int childrenSize = sibling->children.size();
            if(node->getNodeSize() + getKeySize(sibling->keys[keysSize - 1], sibling->attribute) + 4 > PAGE_SIZE) //can't borrow
            {
                return 0;
            }
            node->insertKey(0, parent->keys[position - 1]);
            node->insertChild(0, sibling->children[childrenSize - 1]);

            parent->replaceKey(position - 1, sibling->keys[sibling->keys.size() - 1]);
            
            sibling->keys.erase(sibling->keys.begin() + keysSize - 1, sibling->keys.begin() + keysSize);
            sibling->children.erase(sibling->children.begin() + childrenSize - 1, sibling->children.begin() + childrenSize);
            

            node->writeNodeToPage(ixfileHandle);
            sibling->writeNodeToPage(ixfileHandle);
            parent->writeNodeToPage(ixfileHandle);
        }
    }
    return 0;
}

RC Node::replaceKey(int pos, void *key)
{
     if (this->attrType == TypeInt)
    {
        void *data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        free(this->keys[pos]);
        this->keys[pos] = data;
    }
    else if (this->attrType == TypeReal)
    {
        void * data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
       
        free(this->keys[pos]);
        this->keys[pos] = data;
    }
    else if (this->attrType == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)key, sizeof(int));
        void* data = malloc(nameLength + sizeof(int));
        memcpy(data, &nameLength, sizeof(int));
        memcpy((char *) data + sizeof(int), (char *)key + sizeof(int), nameLength);
        free(this->keys[pos]);
        this->keys[pos] = data;
    }
    return 0;
}

RC Node::printChildren()
{
    cout<<"[print Children]"<<endl;
    cout<<"{";
    for(int i=0;i<this->children.size();i++)
        cout<<children[i]<<"  ";
    cout<<"}"<<endl;
    return 0;
}

int getKeySize(const void* key, const Attribute *attribute)
{
    if (attribute->type == TypeInt || attribute->type == TypeReal)
    {
        return 4;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        
        memcpy(&nameLength, (char *)key, sizeof(int));
        
        return nameLength + 4;
    }
    return 0;
}

int getRidSize(vector<RID> rid)
{
    return 8 * rid.size();
}

int Node::getRightSibling(IXFileHandle &ixfileHandle, Node *parent, int &position)
{
        for(int i=0;i<parent->children.size() - 1;i++)
        {
            if(parent->children[i] == this->cPage)
            {
                position = i;
                return parent->children[i+1];
            }

    }

    return -1;
}

int Node::getLeftSibling(IXFileHandle &ixfileHandle, Node *parent, int &position)
{
    for(int i=0;i<parent->children.size();i++)
    {
        if(parent->children[i] == this->cPage)
        {
            position = i;
            return parent->children[i-1];
        }
    }
    return -1;
}

int Node::deleteRecord(int pos, const RID &rid)
{
    if(this->pointers[pos].size() > 1)
    {
        for(int i=0;i<this->pointers[pos].size();i++)
        {
            if(rid.pageNum == this->pointers[pos][i].pageNum && rid.slotNum == this->pointers[pos][i].slotNum)
            {
                this->pointers[pos].erase(this->pointers[pos].begin() + i, this->pointers[pos].begin() + i + 1);
                return 0;
            }
        }
        return -1;
    }
    else
    {
        if(rid.pageNum == this->pointers[pos][0].pageNum && rid.slotNum == this->pointers[pos][0].slotNum)
        {
            this->pointers.erase(this->pointers.begin() + pos, this->pointers.begin() + pos + 1);
            return 1;
        }
        else return -1;
    }
    return -1;
}

int Node::findKey(const void* key)
{
    int i;
    for (i = 0; i < this->keys.size(); i++)
    {
        if (isEqual(key, this->keys[i], this->attribute))
        {
            return i;
        }
           
    }
    return -1;
}

Node *IndexManager::traverseToLeafNode(IXFileHandle &ixfileHandle, Node *node, const Attribute &attribute) // attribute!!!!!!!!
{
    while (node->nodeType != RootOnly && node->nodeType != LeafNode)
    {
        void *page = malloc(PAGE_SIZE);
        int pageNum = node->children[0];
        ixfileHandle.fileHandle.readPage(node->children[0], page);
        node = new Node(&attribute, page, &ixfileHandle); 
        node->cPage = pageNum;
        free(page);
    }
    return node;
}

int Node::getKeyPosition(const void *key)
{
    for(int i=0;i<this->keys.size();i++)
    {
        if(isEqual(key, this->keys[i],this->attribute)) return i;
    }
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    if (!ixfileHandle.fileHandle.isOpen())
        return 1;
    ix_ScanIterator.attribute = &attribute;
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.ixfileHandle = &ixfileHandle;
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    // printf("[printBtree] atrribute type %d, length: %d\n", attribute.type, attribute.length);
    int numOfPages = ixfileHandle.fileHandle.getNumberOfPages();
    if(numOfPages < 1) return;
    printNode(ixfileHandle, attribute, 0, 0);
    printf("\n");	
}

void IndexManager::printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const int &pageNum, int indent) const
{
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageNum, page);
    Node node(&attribute, page, &ixfileHandle);
    if (node.nodeType == LeafNode || node.nodeType == RootOnly)
    {
        printf("%*s%s", indent, "", "{\n");
        node.printRids(indent + 1);
	printf("\n");
        printf("%*s%s", indent, "", "}");
    }
    else
    {
        printf("%*s%s", indent, "", "{\n");
        printf("%*s%s", indent, "", "\"keys\":[");
        node.printKeys();
        printf("%*s%s", indent, "", "], \n");
        printf("%*s%s", indent, "", "\"children\":[\n");
        for (int i = 0; i < node.children.size(); ++i)
        {
            printNode(ixfileHandle, attribute, node.children[i], indent + 1);
            if (i != node.children.size() - 1)
            {
                printf(",");
            	printf("\n");
            }
        }
        printf("\n");
        printf("%*s%s", indent, "", "]}");
    }
    free(page);
}

IX_ScanIterator::IX_ScanIterator()
{
    this->cPage = 0;
    this->cKey = 0;
    this->cRec = -1;
    this->lastPage = -1;
    this->lastRec = -1;
    this->lastKey = -1;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (!(this->cPage < this->ixfileHandle->fileHandle.getNumberOfPages()))
        return 1;
    if (this->node == NULL)
    {
        void *page = malloc(PAGE_SIZE);
        this->ixfileHandle->fileHandle.readPage(this->cPage, page);
        this->node = new Node(this->attribute, page, this->ixfileHandle);
        this->node->cPage = this->cPage;
        free(page);
    }
    // node->printRids();
    // printf("\n");
#ifdef DEBUG_IX
    printf("[getNextEntry] Total number of pages in file %d\n", this->ixfileHandle->fileHandle.getNumberOfPages());
#endif

    if (this->node->nodeType != RootOnly && this->node->nodeType != LeafNode)
    {
        int cPage = traverseToLeaf(this->node);
        this->cPage = cPage;
    }

#ifdef DEBUG_IX
    int total = this->ixfileHandle->fileHandle.getNumberOfPages();
    printf("[getNextEntry] Finished traversing to leaf\n");
    this->node->printKeys();
    printf("\n");
#endif


    // unsigned readPageCount = 0;
    // unsigned writePageCount = 0;
    // unsigned appendPageCount = 0;
    // this->ixfileHandle->collectCounterValues(readPageCount, writePageCount, appendPageCount);

    // printf("[getNextEntry] readPageCount: %d\n", readPageCount);
    while(1)
    {
        this->cRec++;
        if (this->node->pointers.size() > 0 && this->cRec >= this->node->pointers[this->cKey].size())
        {
            this->cRec = 0;
            this->cKey++;
        }
        if (this->cKey >= this->node->pointers.size() && this->node->next != -1)
        {
#ifdef DEBUG_IX
            printf("[getNextEntry] Current page: %d, loading next page: %d\n", this->node->cPage, this->node->next);
#endif 
            void *page = malloc(PAGE_SIZE);
            this->ixfileHandle->fileHandle.readPage(this->node->next, page);
            this->cPage = this->node->next;
            delete this->node;
            this->node = new Node(this->attribute, page, this->ixfileHandle);
            this->node->cPage = this->cPage;
            this->cRec = 0;
            this->cKey = 0;
            free(page);
        }

        if (this->cKey >= this->node->keys.size() && this->node->next == -1)
        {
#ifdef DEBUG_IX
            printf("[getNextEntry] Current page: %d, loading next page: %d\n", this->node->cPage, this->node->next);
#endif
            return IX_EOF;
        }
#ifdef DEBUG_IX
        printf("[getNextEntry](%d, %d, %d)\n", this->cPage, this->cKey, this->cRec);
#endif
        // else
        // {
        //     this->cRec = 0;
        //     this->cPage = node->next;
        //     this->ixfileHandle->fileHandle.readPage(this->cPage, page);
        //     node = new Node(this->attribute, page);
        // }

        if (this->cKey >= this->node->keys.size() || this->cRec >= this->node->pointers[this->cKey].size())
            continue;

        if (lowKey != NULL)
            if (!((this->lowKeyInclusive && isLargerAndEqualThan(this->attribute, node->keys[this->cKey], this->lowKey)) || 
                (!this->lowKeyInclusive && isLargerThan(this->attribute, node->keys[this->cKey], this->lowKey))))
                continue;
        if (highKey != NULL)
            if (!((this->highKeyInclusive && isLessAndEqualThan(this->attribute, node->keys[this->cKey], this->highKey)) || 
                (!this->highKeyInclusive && isLessThan(this->attribute, node->keys[this->cKey], this->highKey))))
            {
                return IX_EOF;        
            }
        rid.pageNum = this->node->pointers[this->cKey][this->cRec].pageNum;
        rid.slotNum = this->node->pointers[this->cKey][this->cRec].slotNum;
        if (this->attribute->type == TypeInt || this->attribute->type == TypeReal)
            memcpy(key, this->node->keys[this->cKey], this->attribute->length);
        else
        {
            int nameLength;
            memcpy(&nameLength, (char *)this->node->keys[this->cKey], sizeof(int));
            memcpy(key, &nameLength, sizeof(int));
            memcpy((char *)key + sizeof(int), (char *)this->node->keys[this->cKey] + sizeof(int), nameLength);
        }

        if (this->lastPage == this->cPage)
        {
            void *page = malloc(PAGE_SIZE);
            this->ixfileHandle->fileHandle.readPage(this->cPage, page);
            delete this->node;
            this->node = new Node(this->attribute, page, this->ixfileHandle);
            this->node->cPage = this->cPage;
            free(page);
            // if(this->lastKey >= this->node->pointers.size() || this->lastRec >= this->node->pointers)
            if (!(this->node->pointers[lastKey][lastRec].pageNum == this->previousRid.pageNum && this->node->pointers[lastKey][lastRec].slotNum == this->previousRid.slotNum))
            {
                this->cKey = this->lastKey;
                this->cPage = this->lastPage;
                this->cRec = this->lastRec;
            }
        }

        this->previousRid = rid;
        this->lastKey = this->cKey;
        this->lastPage= this->cPage;
        this->lastRec = this->cRec;
        break;
    }
    return 0;
}

int IX_ScanIterator::traverseToLeaf(Node *&node)
{
    int cPage = 0;
    while (node->nodeType != RootOnly && node->nodeType != LeafNode)
    {
        int pos;
        if (this->lowKey == NULL)
            pos = 0;
        else        
            pos = node->getChildPos(this->lowKey);
#ifdef DEBUG_IX
        printf("[traverseToLeaf] Current page number %d, next pageNum: %d\n", node->cPage, node->children[pos]);
#endif
        cPage = node->children[pos];
        // node->printKeys();
        void *page = malloc(PAGE_SIZE);
        this->ixfileHandle->fileHandle.readPage(node->children[pos], page);
        delete node;
        node = new Node(this->attribute, page, this->ixfileHandle);
#ifdef DEBUG_IX
        node->printKeys();
        printf("\npos: %d\n", pos);
#endif
        free(page);
    }
    // node->printKeys();
    return cPage;
}

RC IX_ScanIterator::close()
{
    this->cPage = 0;
    this->cKey = 0;
    this->cRec = -1;

    if (this->node != NULL)
    {
        delete this->node;
        this->node = NULL;
    }
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
    fileHandle.closeFile();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    fileHandle.collectCounterValues(this->ixReadPageCounter, this->ixWritePageCounter, this->ixAppendPageCounter);
    readPageCount = this->ixReadPageCounter;
    writePageCount = this->ixWritePageCounter;
    appendPageCount = this->ixAppendPageCounter;
    return 0;
}

Node::Node(const Attribute &attribute)
{
    this->attribute = &attribute;
    this->attrType = attribute.type;
    this->size = sizeof(NodeType) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int);
    // NodeType, previous pointer, next pointer, # of keys, # of children, overflow page
}

Node::Node(const Attribute *attribute)
{
    this->attribute = attribute;
    this->attrType = attribute->type;
    this->size = sizeof(NodeType) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int);
}


Node::Node(const Attribute *attribute, const void *page, IXFileHandle *ixfileHandle)
{
    this->isLoaded = true;
    this->attrType = attribute->type;
    this->attribute = attribute;
    int offset = 0;
    memcpy(&this->nodeType, (char *)page + offset, sizeof(NodeType));
    offset += sizeof(NodeType);
    memcpy(&this->previous, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&this->next, (char *)page + offset, sizeof(int));
    offset += sizeof(int);

    int nKeys;
    memcpy(&nKeys, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nKeys; ++i)
    {
        if (this->attrType == TypeInt)
        {
            void * data = malloc(attribute->length);
            memcpy(data, (char *)page + offset, sizeof(attribute->length));
            offset += sizeof(attribute->length);
            this->keys.push_back(data);
        }
        else if (this->attrType == TypeReal)
        {
            void * data = malloc(attribute->length);
            memcpy(data, (char *)page + offset, sizeof(attribute->length));
            offset += sizeof(attribute->length);
            this->keys.push_back(data);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
            // printf("String length: %d\n", nameLength);
            void* data = malloc(nameLength + sizeof(int));
            memcpy(data, &nameLength, sizeof(int));
            memcpy((char *) data + sizeof(int), (char *)page + offset, nameLength);
            offset += nameLength;
            this->keys.push_back(data);
        }
    }

    if (this->nodeType == LeafNode || this->nodeType == RootOnly)
    {
        int nRids;
        memcpy(&nRids, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
#ifdef DEBUG_IX
        // printf("[Node] nRids: %d, offset: %d\n", nRids, offset - sizeof(int));
#endif
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords;
            memcpy(&nRecords, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
#ifdef DEBUG_IX
            // printf("[Node] nRecords %d, offset: %d\n", nRecords, offset - sizeof(int));
#endif
            vector <RID> records;
            for (int j = 0; j < nRecords; ++j)
            {
                RID rid;
                memcpy(&rid.pageNum, (char *)page + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rid.slotNum, (char *)page + offset, sizeof(int));
                offset += sizeof(int);
                records.push_back(rid);
            }
            this->pointers.push_back(records);
        }
    }
    else
    {
        int nChildren;
        memcpy(&nChildren, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nChildren; ++i)
        {
            int value;
            memcpy(&value, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
            this->children.push_back(value);
        }
    }

    int overFlowPage;
    memcpy(&overFlowPage, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    if (this->nodeType == RootOnly || this->nodeType == RootNode)
        this->cPage = 0;
    if (overFlowPage != -1)
    {
        this->isOverflow = true;
        this->overFlowPages.push_back(overFlowPage);
        this->deserializeOverflowPage(overFlowPage, ixfileHandle);
    }
    // this->size = offset;
}

RC Node::deserializeOverflowPage(int nodeId, IXFileHandle *ixfileHandle)
{
    void *page = malloc(PAGE_SIZE);
    ixfileHandle->fileHandle.readPage(nodeId, page);
    int offset = 0;
    int nRids;
    memcpy(&nRids, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
#ifdef DEBUG_IX
    printf("[deserialize] nRids in overflow page: %d\n", nRids);
#endif
    for (int i = 0; i < nRids; ++i)
    {
        RID rid;
        memcpy(&rid.pageNum, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
        memcpy(&rid.slotNum, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
        this->pointers[0].push_back(rid);
    }
    int overFlowPage;
    memcpy(&overFlowPage, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
#ifdef DEBUG_IX
    printf("[deserialize] offset: %d, next overflow page: %d\n", offset, overFlowPage);
#endif
    if (overFlowPage != -1)
    {
        this->overFlowPages.push_back(overFlowPage);
        this->deserializeOverflowPage(overFlowPage, ixfileHandle);
    }
    free(page);
    return 0;
}

RC Node::serialize(void * page)
{
    int offset = 0;
    memcpy((char *)page + offset, &this->nodeType, sizeof(NodeType));
    offset += sizeof(NodeType);
    memcpy((char *)page + offset, &this->previous, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)page + offset, &this->next, sizeof(int));
    offset += sizeof(int);

    int nKeys = this->keys.size();
    memcpy((char *)page + offset, &nKeys, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nKeys; ++i)
    {
        if (this->attrType == TypeInt)
        {
            memcpy((char *)page + offset, this->keys[i], sizeof(attribute->length));
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeReal)
        {
            memcpy((char *)page + offset, this->keys[i], sizeof(attribute->length));
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, this->keys[i], sizeof(int));
            memcpy((char *)page + offset, &nameLength, sizeof(int));
            offset += sizeof(int);
            // printf("String length: %d\n", nameLength);
            memcpy((char *)page + offset, (char *) this->keys[i] + sizeof(int), nameLength);
            offset += nameLength;
        }
    }

    if (this->nodeType == LeafNode || this->nodeType == RootOnly)
    {
        int nRids = this->pointers.size();
#ifdef DEBUG_IX
       // printf("[serialize] nPointers %d, offset: %d\n", pointers.size(), offset);
#endif
        memcpy((char *)page + offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords = pointers[i].size();
            memcpy((char *)page + offset, &nRecords, sizeof(int));
            offset += sizeof(int);
#ifdef DEBUG_IX
          //  printf("[serialize] nRecords %d, offset: %d\n", pointers[i].size(), offset - sizeof(int));
#endif
            for (int j = 0; j < nRecords; ++j)
            {
                int pageNum = this->pointers[i][j].pageNum;
                int slotNum = this->pointers[i][j].slotNum;
                memcpy((char *)page + offset, &pageNum, sizeof(int));
                offset += sizeof(int);
                memcpy((char *)page + offset, &slotNum, sizeof(int));
                offset += sizeof(int);
#ifdef DEBUG_IX
        //        printf("[serialize] Record (%d, %d) %d\n", pageNum, slotNum, offset);
#endif
            }
        }
    }
    else
    {
        int nChildren = this->children.size();
        memcpy((char *)page + offset, &nChildren, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nChildren; ++i)
        {
            int value = this->children[i];
            memcpy((char *)page + offset, &value, sizeof(int));
            offset += sizeof(int);
        }
    }

    int overFlowPage = -1;
    memcpy((char *)page + offset, &overFlowPage, sizeof(int));
    offset += sizeof(int);
    return 0;
}

int Node::serializeOverflowPage(int start, int end, void *page)
{
#ifdef DEBUG_IX
    printf("[serializeOverflowPage] Overflow page contains Rids from %d to %d\n", start, end - 1);
#endif
    int nRids = end - start;
    int offset = 0;
    memcpy((char *)page + offset, &nRids, sizeof(int));
    offset += sizeof(int);
    for (int i = start; i < end; ++i)
    {
        int pageNum = this->pointers[0][i].pageNum;
        int slotNum = this->pointers[0][i].slotNum;
        memcpy((char *)page + offset, &pageNum, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)page + offset, &slotNum, sizeof(int));
        offset += sizeof(int);
    }
    return offset;
}

int Node::getHeaderAndKeysSize()
{
    int offset = 0;
    offset += sizeof(NodeType);
    offset += sizeof(int);
    offset += sizeof(int);

    int nKeys = this->keys.size();
    offset += sizeof(int);
    for (int i = 0; i < nKeys; ++i)
    {
        if (this->attrType == TypeInt)
        {
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeReal)
        {
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            offset += sizeof(int);
            memcpy(&nameLength, this->keys[i], sizeof(int));
            offset += nameLength;
        }
    }
    offset += sizeof(int); //overflow pointer
    offset += sizeof(int); //number of vectors of Rids
    offset += sizeof(int); //number of Rids in the vector
    return offset;
}

int Node::getNodeSize()
{
    int offset = 0;
    offset += sizeof(NodeType);
    offset += sizeof(int);
    offset += sizeof(int);

    int nKeys = this->keys.size();
    offset += sizeof(int);
    for (int i = 0; i < nKeys; ++i)
    {
        if (this->attrType == TypeInt)
        {
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeReal)
        {
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            offset += sizeof(int);
            memcpy(&nameLength, this->keys[i], sizeof(int));
#ifdef DEBUG_IX
            printf("String length: %d\n", nameLength);
#endif
            offset += nameLength;
        }
    }

    if (this->nodeType == LeafNode || this->nodeType == RootOnly)
    {
        int nRids = this->pointers.size();
#ifdef DEBUG_IX
       // printf("[serialize] nPointers %d, offset: %d\n", pointers.size(), offset);
#endif
        offset += sizeof(int);
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords = pointers[i].size();
            offset += sizeof(int);
#ifdef DEBUG_IX
          //  printf("[serialize] nRecords %d, offset: %d\n", pointers[i].size(), offset - sizeof(int));
#endif
            for (int j = 0; j < nRecords; ++j)
            {
                int pageNum = this->pointers[i][j].pageNum;
                int slotNum = this->pointers[i][j].slotNum;
                offset += sizeof(int);
                offset += sizeof(int);
#ifdef DEBUG_IX
        //        printf("[serialize] Record (%d, %d) %d\n", pageNum, slotNum, offset);
#endif
            }
        }
    }
    else
    {
        int nChildren = this->children.size();
        offset += sizeof(int);
        for (int i = 0; i < nChildren; ++i)
        {
            int value = this->children[i];
            offset += sizeof(int);
        }
    }
    offset += sizeof(int);
    this->size = offset;
    return offset;
}

RC Node::appendKey(const void* key)
{
     #ifdef DEBUG_IX
    printf(".....apending keys, type: %d\n", this->attrType);
    printf(".....apend keys int, value: %d\n");
    #endif
    if (this->attrType == TypeInt)
    {
       
        void * data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        this->keys.push_back(data);
        // this->printKeys();
        // printf("\n");
    }
    else if (this->attrType == TypeReal)
    {
        void * data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        this->keys.push_back(data);        
    }
    else if (this->attrType == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)key, sizeof(int));
        // printf("String length: %d\n", nameLength);
        void* data = malloc(nameLength + sizeof(int));
        memcpy(data, &nameLength, sizeof(int));
        memcpy((char *) data + sizeof(int), (char *)key + sizeof(int), nameLength);
        this->keys.push_back(data);        
    }
    return 0;
}

RC Node::appendChild(int pageNum)
{
    int data;
    memcpy(&data, &pageNum, sizeof(int));
    this->children.push_back(data);
    return 0;
}

RC Node::appendPointer(vector<RID> rids)
{
    vector<RID> vector_rid;
    for(int i=0;i<rids.size();i++)
    {
    RID data_rid;
    int data_rid_pageNum;
    int data_rid_slotNum;
    memcpy(&data_rid_pageNum, &rids[i].pageNum, sizeof(int));
    memcpy(&data_rid_slotNum, &rids[i].slotNum, sizeof(int));
    data_rid.pageNum = data_rid_pageNum;
    data_rid.slotNum = data_rid_slotNum;

    vector_rid.push_back(data_rid);
    }
    this->pointers.push_back(vector_rid);
    return 0;
}

Node::~Node()
{
    for (int i = 0; i < this->keys.size(); ++i)
    {
        free(keys[i]);
    }
}

RC Node::printKeys()
{
    for (int i = 0; i < this->keys.size(); ++i)
    {
        if (this->attrType == TypeInt)
        {
            int value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("\"%d\"", value);
        }
        else if (this->attrType == TypeReal)
        {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("\"%f\"", value);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)this->keys[i], sizeof(int));
 #ifdef DEBUG_IX
            printf("[printKeys] String length: %d\n", nameLength);
 #endif
            char* value_c = (char *)malloc(nameLength + 1);
            memcpy(value_c, (char *)this->keys[i] + sizeof(int), nameLength);
            value_c[nameLength] = '\0';
            printf("\"%s\"", value_c);
            free(value_c);
        }

        if (i != this->keys.size() - 1)
            printf(",");
    }
    return 0;
}

RC Node::printRids(int indent)
{
    printf("%*s%s", indent, "", "\"keys\":[");
    for (int i = 0; i < this->keys.size(); ++i)
    {
        printf("\"");
        if (this->attrType == TypeInt)
        {
            int value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
#ifdef DEBUG_IX
            printf("[printRids] nPointers %d\n", pointers.size());
#endif
            printf("%d:", value);
        }
        else if (this->attrType == TypeReal)
        {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("%f:", value);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)this->keys[i], sizeof(int));
            // printf("String length: %d\n", nameLength);
            char* value_c = (char *)malloc(nameLength + 1);
            memcpy(value_c, (char *)this->keys[i] + sizeof(int), nameLength);
            value_c[nameLength] = '\0';
            printf("%s:", value_c);
            free(value_c);
        }

        printf("[");
        for (int j = 0; j < this->pointers[i].size(); ++j)
        {
            printf("(%d, %d)", this->pointers[i][j].pageNum, this->pointers[i][j].slotNum);
            if (j != this->pointers[i].size() - 1)
                printf(",");
        }
        printf("]\"");
        if (i != this->keys.size() - 1)
            printf(",");
    }
    printf("]");
    return 0;
}

bool Node::isFull()
{
    if(this->getNodeSize() > PAGE_SIZE)
        return true;
    else
        return false;
}

bool Node::isLessHalfFull()
{
    if (this->getNodeSize() < PAGE_SIZE / 2)
        return true;
    else
        return false;
}


int Node::getChildPos(const void* value)
{
    int i;
    for (i = 0; i < this->keys.size(); ++i)
    {
        if (isLessThan(this->attribute, value, this->keys[i]))
            break;
    }
    return i;
}

bool isLessThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        //printf("[isLessThan] comparing %d, %d", value, key);
        return value < key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value < key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value < key;
    }    
}

bool isLessAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value <= key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value <= key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value <= key;
    }    
}

bool isLargerAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value >= key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value >= key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value >= key;
    }    
}

bool isLargerThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value > key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value > key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value > key;
    }    
}

bool isEqual(const void* value1, const void* value2, const Attribute *attribute)
{
    if (attribute->type == TypeInt)
    {
        int cmp_value1;
        int cmp_value2;
        memcpy(&cmp_value1, (char *)value1, sizeof(int));
        memcpy(&cmp_value2, (char *)value2, sizeof(int));
        return cmp_value1 == cmp_value2;
    }
    else if (attribute->type == TypeReal)
    {
        float cmp_value1;
        float cmp_value2;
        memcpy(&cmp_value1, (char *)value1, sizeof(attribute->length));
        memcpy(&cmp_value2, (char *)value2, sizeof(attribute->length));
        return cmp_value1 == cmp_value2;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)value1, sizeof(int));
        char* cmp_value1_c = (char *)malloc(nameLength + 1);
        memcpy(cmp_value1_c, (char *)value1 + sizeof(int), nameLength);
        cmp_value1_c[nameLength] = '\0';
        string cmp_value1 = string(cmp_value1_c);

        memcpy(&nameLength, (char *)value2, sizeof(int));
        char* cmp_value2_c = (char *)malloc(nameLength + 1);
        memcpy(cmp_value2_c, (char *)value2 + sizeof(int), nameLength);
        cmp_value2_c[nameLength] = '\0';
        string cmp_value2 = string(cmp_value2_c);
#ifdef DEBUG_IX
        printf("[isEqual] Comparing %s, %s\n", cmp_value1.c_str(), cmp_value2.c_str());
        printf("[isEqual] %d\n", cmp_value1 == cmp_value1);
#endif
        free(cmp_value1_c);
        free(cmp_value2_c);
        return cmp_value1 == cmp_value2;
    }
}
