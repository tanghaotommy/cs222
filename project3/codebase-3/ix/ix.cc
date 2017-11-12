
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
    if (rc == 0)
        ixfileHandle.fileHandle.collectCounterValues(ixfileHandle.ixReadPageCounter, 
            ixfileHandle.ixWritePageCounter, ixfileHandle.ixAppendPageCounter);
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    cout<<"-------before insert--------"<<endl;
    this-> printBtree(ixfileHandle,attribute);
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
        Node root = Node(&attribute, page);
        // free(page);
        if (root.nodeType == RootOnly)
        {
            
            int pos = root.getChildPos(key);
            //printNode(ixfileHandle, attribute, 0);
            root.insertKey(pos, key); //Insert into vector.
            root.insertPointer(pos, rid, key); //Insert into vector.
            vector<Node*> path;
            path.push_back(&root);
            if (root.pointers.size() > 2*root.order)
            {
                #ifdef DEBUG_IX
                printf("[insertEntry] RootOnly split\n");
                #endif
                split(path, ixfileHandle);                            
            } else
            {   
                #ifdef DEBUG_IX
                printf("[insertEntry] RootOnly write to %d page\n", root.cPage);
                #endif
                realloc(page, PAGE_SIZE);
                root.serialize(page);
                int rc = ixfileHandle.fileHandle.writePage(root.cPage, page);
                // this->printBtree(ixfileHandle, attribute);  
                free(page);
                return 0;
            }
        } 
        else
        {
            vector<Node*> path;
            traverseToLeafWithPath(ixfileHandle, &root, path, key, attribute);
            
            int pos = path[path.size() - 1]->getChildPos(key);
            path[path.size() - 1]->insertKey(pos, key); //Insert into vector.
            path[path.size() - 1]->insertPointer(pos, rid, key); //Insert into vector.
            if (path[path.size() - 1]->keys.size() > 2*path[path.size() - 1]->order)
            {
                cout<<"[Split]"<<endl;
                split(path, ixfileHandle);                            
            }
            else {
                void *page = malloc(PAGE_SIZE);
                path[path.size() - 1]->serialize(page);
                ixfileHandle.fileHandle.writePage(path[path.size() - 1]->cPage, page);
                free(page);
            }
        }
    }
        cout<<"-------after insert--------"<<endl;
    this-> printBtree(ixfileHandle,attribute);
    //this->printBtree(ixfileHandle, attribute); 
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
    Node *node = new Node(&attribute, page);   
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
        int order = node->order; 

        Node new_leaf = Node(node->attribute);
        new_leaf.nodeType = LeafNode;

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
        node->next = new_leaf.cPage;
        writeNodeToPage(ixfileHandle, node);
        new_leaf.previous = node->cPage;
        writeNodeToPage(ixfileHandle, &new_leaf);
        path.pop_back();
        Node *parent = path[path.size() - 1];

        int pos = parent->getChildPos(new_leaf.keys[0]);
        parent->insertKey(pos, new_leaf.keys[0]); 

        parent->insertChild(pos + 1, new_leaf.cPage); 
        
        if(parent->keys.size() > 2 * parent->order)
        {
            split(path, ixfileHandle);            
        }
        else writeNodeToPage(ixfileHandle, parent);
    }
    else if(node->nodeType == InternalNode)
    {
        int order = node->order;  
        Node new_intermediate(node->attribute);
        path.pop_back();
        Node *parent = path[path.size() - 1];

        new_intermediate.nodeType = node->nodeType;

        for(int i = order + 1;i < node->keys.size();i++)
        {
            new_intermediate.appendKey(node->keys[i]);
        }
        for(int i = order + 1;i < node->children.size();i++)
        {
            new_intermediate.appendChild(node->children[i]);
        }

        void *page = malloc(PAGE_SIZE);
        ixfileHandle.fileHandle.appendPage(page);
        new_intermediate.cPage = ixfileHandle.fileHandle.getNumberOfPages() - 1;
        free(page);
        int pos = parent->getChildPos(node->keys[order]);
        parent->insertKey(pos, node->keys[0]); 

        parent->insertChild(pos + 1, new_intermediate.cPage);  //TODO Always pos + 1? what if it's first child?

        writeNodeToPage(ixfileHandle, &new_intermediate);
        node->keys.erase(node->keys.begin() + order,node->keys.begin() + node->keys.size());
        node->children.erase(node->children.begin() + order + 1,node->children.begin() + node->children.size());
        writeNodeToPage(ixfileHandle, node);
        
        if(parent->keys.size() > 2 * parent->order)
        {
            split(path, ixfileHandle);            
        }
        else writeNodeToPage(ixfileHandle, parent);
    }
    else if(node->nodeType == RootOnly)
    {
        int order = node->order;  
        Node new_leafNode1(node->attribute);
        Node new_leafNode2(node->attribute);
        node->nodeType = RootNode;
        new_leafNode1.nodeType = LeafNode;
        new_leafNode2.nodeType = LeafNode;
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
        writeNodeToPage(ixfileHandle, node);

        new_leafNode1.next = new_leafNode2.cPage;
        new_leafNode2.previous = new_leafNode1.cPage;
        writeNodeToPage(ixfileHandle, &new_leafNode1);
        writeNodeToPage(ixfileHandle, &new_leafNode2);

    }
    else if(node->nodeType == RootNode)
    {
        int order = node->order;  
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
        writeNodeToPage(ixfileHandle, node);

        writeNodeToPage(ixfileHandle, &new_intermediate1);
        writeNodeToPage(ixfileHandle, &new_intermediate2);
    }
    return 0;
}

RC writeNodeToPage(IXFileHandle &ixfileHandle, Node *node){
    void *page = malloc(PAGE_SIZE);
    node->serialize(page);
    ixfileHandle.fileHandle.writePage(node->cPage, page);
    free(page);
    return 0;
}

RC Node::insertKey(int pos, const void* key)
{
    #ifdef DEBUG_IX
    printf("[insertKey]inserting keys, type: %d, pos : %d, total number of keys: %d\n", this->attrType, pos, this->keys.size());
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
    }
    else if (this->attrType == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)key, sizeof(int));
        // printf("String length: %d\n", nameLength);
        void* data = malloc(nameLength + sizeof(int));
        memcpy(data, &nameLength, sizeof(int));
        memcpy((char *) data + sizeof(int), (char *)key + sizeof(int), nameLength);
        if(this->keys.size() >= 1  && pos >= 1 && isEqual(this->keys[pos - 1], data, this->attribute))  
        {
            free(data);
            return 0;
        } 
        this->keys.insert(this->keys.begin() + pos, data);        
    }
    return 0;
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
    this->printRids();
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
    
    void *page = malloc(PAGE_SIZE);
    int pageNum = 0;  //need to finde the page
    ixfileHandle.fileHandle.readPage(pageNum, page);
    Node node(&attribute, page);
    int pos = node.getKeyPosition(key);
    if(pos == -1) return -1;
    if(node.pointers[pos].size() > 1){
        for(int i=0;i<node.pointers[pos].size();i++)
        {
            if(node.pointers[pos][i].pageNum == rid.pageNum && node.pointers[pos][i].slotNum == rid.slotNum)
            {
                node.pointers[pos].erase(node.pointers[pos].begin() + i, node.pointers[pos].begin() + i + 1);
                break;
            }
            else return -1;
        }
    }
    else
    {
        node.pointers.erase(node.pointers.begin() + pos, node.pointers.begin() + pos + 1);
        node.keys.erase(node.keys.begin() + pos, node.keys.begin() + pos + 1);
    }
    if(node.keys.size() < node.order)
    {
        //merge
    }
    else{
        writeNodeToPage(ixfileHandle, &node);
    }
    
    return 0;
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
    cout<<"[print Btree]"<<"Pages"<<numOfPages<<endl;
        printNode(ixfileHandle, attribute, 0);
}

void IndexManager::printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const int &pageNum) const
{
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageNum, page);
    Node node(&attribute, page);
    if (node.nodeType == LeafNode || node.nodeType == RootOnly)
    {
        printf("{\n");
        node.printRids();
        printf("\n}\n");
    }
    else
    {
        printf("{\n");
        printf("\"keys\":[");
        node.printKeys();
        printf("], \n");
        printf("\"children\":[\n");
        for (int i = 0; i < node.children.size(); ++i)
        {
            printNode(ixfileHandle, attribute, node.children[i]);
            if (i != node.children.size() - 1)
                printf(",");
            printf("\n");
        }
        printf("]");
        printf("}\n");
    }
    free(page);
}

IX_ScanIterator::IX_ScanIterator()
{
    this->cPage = 0;
    this->cKey = 0;
    this->cRec = -1;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (!this->cPage < this->ixfileHandle->fileHandle.getNumberOfPages())
        return 1;
    void *page = malloc(PAGE_SIZE);
    this->ixfileHandle->fileHandle.readPage(this->cPage, page);
    Node *node = new Node(this->attribute, page);
    // node->printRids();
    // printf("\n");

    if (node->nodeType != RootOnly && node->nodeType != LeafNode)
    {
        traverseToLeaf(node);
        this->cPage = node->cPage;
    }
    while(1)
    {
        this->cRec++;
        if (this->cRec >= node->pointers[this->cKey].size())
        {
            this->cRec = 0;
            this->cKey++;
        }
        if (this->cKey >= node->pointers.size() && node->next != -1)
        {
            void *page = malloc(PAGE_SIZE);
            this->ixfileHandle->fileHandle.readPage(node->next, page);
            node = new Node(this->attribute, page);
            this->cRec = 0;
            this->cKey = 0;
        }

        if (this->cKey >= node->keys.size() && node->next == -1)
        {
            delete node;
            free(page);
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
        if (lowKey != NULL)
            if (!((this->lowKeyInclusive && isLargerAndEqualThan(this->attribute, node->keys[this->cKey], this->lowKey)) || 
                (!this->lowKeyInclusive && isLargerThan(this->attribute, node->keys[this->cKey], this->lowKey))))
                continue;
        if (highKey != NULL)
            if (!((this->highKeyInclusive && isLessAndEqualThan(this->attribute, node->keys[this->cKey], this->highKey)) || 
                (!this->highKeyInclusive && isLessThan(this->attribute, node->keys[this->cKey], this->highKey))))
            {
                delete node;
                free(page);
                return IX_EOF;        
            }
        rid.pageNum = node->pointers[this->cKey][this->cRec].pageNum;
        rid.slotNum = node->pointers[this->cKey][this->cRec].slotNum;
        if (this->attribute->type == TypeInt || this->attribute->type == TypeReal)
            memcpy(key, node->keys[this->cKey], this->attribute->length);
        else
        {
            int nameLength;
            memcpy(&nameLength, (char *)node->keys[this->cKey], sizeof(int));
            memcpy(key, &nameLength, sizeof(int));
            memcpy((char *)key + sizeof(int), (char *)node->keys[this->cKey] + sizeof(int), nameLength);
        }
        break;
    }
    delete node;
    free(page);
    return 0;
}

RC IX_ScanIterator::traverseToLeaf(Node *node)
{
    while (node->nodeType != RootOnly && node->nodeType != LeafNode)
    {
        int pos = node->getChildPos(this->lowKey);
        void *page = malloc(PAGE_SIZE);
        this->ixfileHandle->fileHandle.readPage(node->children[pos], page);
        node = new Node(this->attribute, page);
        free(page);
    }
    return 0;
}

RC IX_ScanIterator::close()
{
    this->cPage = 0;
    this->cKey = 0;
    this->cRec = -1;
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
}

Node::Node(const Attribute *attribute)
{
    this->attribute = attribute;
    this->attrType = attribute->type;
}


Node::Node(const Attribute *attribute, const void *page)
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
        printf("[Node] nRids: %d, offset: %d\n", nRids, offset - sizeof(int));
#endif
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords;
            memcpy(&nRecords, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
#ifdef DEBUG_IX
            printf("[Node] nRecords %d, offset: %d\n", nRecords, offset - sizeof(int));
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
    if (this->nodeType == RootOnly || this->nodeType == RootNode)
        this->cPage = 0;
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

    return 0;
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
    memcpy(&data, (char *)pageNum, sizeof(int));
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
            printf("%d", value);
        }
        else if (this->attrType == TypeReal)
        {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("%f", value);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)this->keys[i], sizeof(int));
            // printf("String length: %d\n", nameLength);
            char* value_c = (char *)malloc(nameLength + 1);
            memcpy(value_c, (char *)this->keys[i] + sizeof(int), nameLength);
            value_c[nameLength] = '\0';
            printf("\"%s\"\n", value_c);
            free(value_c);
        }

        if (i != this->keys.size() - 1)
            printf(",");
    }
    return 0;
}

RC Node::printRids()
{
    printf("\"keys\":[");
    for (int i = 0; i < this->keys.size(); ++i)
    {
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
            printf("\"%s:", value_c);
            free(value_c);
        }

        printf("[");
        for (int j = 0; j < this->pointers[i].size(); ++j)
        {
            printf("(%d, %d)", this->pointers[i][j].pageNum, this->pointers[i][j].slotNum);
            if (i != this->keys.size() - 1)
                printf(",");
        }
        printf("]");
        if (this->attrType == TypeVarChar)
            printf("\"");
        if (i != this->keys.size() - 1)
            printf(",");
    }
    printf("]");
    return 0;
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
        free(cmp_value1_c);
        free(cmp_value2_c);
        return cmp_value1 == cmp_value1;
    }
}