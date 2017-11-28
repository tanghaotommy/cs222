#include "rbfm.h"
#include <math.h>
#include <stdlib.h> 

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

bool hasDeletedSlot(void *page)
{
    int nSlot;
    int insertSlot;
    //Get total number of slots
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    int offset;
    int lastLength;
    for (int i = 0; i < nSlot; ++i)
    {
        memcpy(&offset, (char *)page + PAGE_SIZE - sizeof(int) - i * sizeof(int), sizeof(int));
        if (offset == -1)
            return true;
    }
    return false;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int nFields = recordDescriptor.size();
    // int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    // printf("Number of fields: %d, The data is: %s\n", nFields, data);
    void *page = malloc(PAGE_SIZE);
    int cPage = fileHandle.getNumberOfPages() - 1;
    fileHandle.readPage(cPage, page);
    // this->printRecord(recordDescriptor, data);

    // //length + data
    // int offset = 0;

    // //Get next available position in current page
    // while ( cPage >= 0 && offset < PAGE_SIZE)
    // {
    //     int recordLength;
    //     memcpy(&recordLength, (char *)page + offset, sizeof(int));
    //     // printf("offset pos: %d, recordLength: %d\n", offset, recordLength);
    //     if (recordLength != 0) 
    //     {
    //         offset += sizeof(int);
    //         offset += recordLength;
    //     }
    //     else
    //         break;
    // }

    // int recordLength = this->getRecordLength(recordDescriptor, data);

    // //If there is nothing in the file or not enough space
    // if ( cPage < 0 || PAGE_SIZE - 1 - offset - sizeof(int) < recordLength)
    // {
    //     offset = 0;
    //     cPage++;
    //     for (int i = 0; i < PAGE_SIZE; ++i)
    //     {
    //         ((char *)page)[i] = 0;
    //     }
    //     memcpy((char *)page, &recordLength, sizeof(int));
    //     memcpy((char *)page + sizeof(int), data, recordLength);
    //     fileHandle.appendPage(page);
    // }
    // else
    // {
    //     memcpy((char *)page + offset, &recordLength, sizeof(int));
    //     memcpy((char *)page + offset + sizeof(int), data, recordLength);
    //     fileHandle.writePage(cPage, page);
    // }
    // rid.pageNum = cPage;
    // rid.slotNum = offset;
    // delete page;
    // return 0;

    //total + (length, data)
    // int total;
    // memcpy(&total, (char *)page, sizeof(int));   
    // int recordLength = this->getRecordLength(recordDescriptor, data);
    // if ( total == 0 || PAGE_SIZE - 1 - total - sizeof(int) < recordLength) {
    //     cPage++;
    //     for (int i = 0; i < PAGE_SIZE; ++i)
    //     {
    //         ((char *)page)[i] = 0;
    //     }
    //     total = 0;
    //     total += sizeof(int);
    //     rid.slotNum = sizeof(int);
    //     total += sizeof(int) + recordLength;
    //     memcpy((char *)page, &total, sizeof(int));
    //     memcpy((char *)page + sizeof(int), &recordLength, sizeof(int));
    //     memcpy((char *)page + sizeof(int) + sizeof(int), data, recordLength);
    //     fileHandle.appendPage(page);       
    // }
    // else
    // {
    //     rid.slotNum = total;
    //     memcpy((char *)page + total, &recordLength, sizeof(int));
    //     memcpy((char *)page + total + sizeof(int), data, recordLength);

    //     total += sizeof(int) + recordLength;
    //     memcpy((char *)page, &total, sizeof(int));
    //     fileHandle.writePage(cPage, page);        
    // }
    // rid.pageNum = cPage;
    // // printf("pageNum: %d, slotNum: %d, length: %d\n", rid.pageNum, rid.slotNum, recordLength);
    // delete page;
    // return 0;    

    int total;
    int *indexList = new int[nFields];
    this->getIndexList(recordDescriptor, data, indexList);

    memcpy(&total, (char *)page, sizeof(int));   
    if (cPage < 0)
        total = 0;
    int recordLength = this->getRecordLength(recordDescriptor, data);
    int left = PAGE_SIZE - total - sizeof(int) - nFields * sizeof(int) - sizeof(int);

    if (total != 0)
    {
        if(hasDeletedSlot(page))
            left += sizeof(int);
    }
    else
        left = PAGE_SIZE;

    if (left < recordLength)
    {
        for (cPage = 0; cPage < fileHandle.getNumberOfPages(); ++cPage)
        {
            fileHandle.readPage(cPage, page);
            memcpy(&total, (char *)page, sizeof(int));
            left = PAGE_SIZE - total - sizeof(int) - nFields * sizeof(int) - sizeof(int);
            if (hasDeletedSlot(page))
                left += sizeof(int);
            if (left >= recordLength)
                break;
        }
        if (cPage == fileHandle.getNumberOfPages())
        {
            total = 0;
            cPage = fileHandle.getNumberOfPages() - 1;
        }
    }
#ifdef DEBUG
    printf("[insertRecord] total: %d\n", total);
#endif
    if ( total == 0)
    {
#ifdef DEBUG
        printf("[insertRecord] append page\n");
#endif
        cPage++;
        for (int i = 0; i < PAGE_SIZE; ++i)
        {
            ((char *)page)[i] = 0;
        }
        total = 0;
        total += sizeof(int);

        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + total + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        // total += nFields * sizeof(int);
        rid.slotNum = 0;
        recordLength += (nFields + 1) * sizeof(int);
        total += (2 * sizeof(int) + recordLength);
        // total += sizeof(int) * 2;
        memcpy((char *)page, &total, sizeof(int));
        memcpy((char *)page + sizeof(int), &recordLength, sizeof(int));
        // printf("adding length: %d, total: %d\n", recordLength, total);
        memcpy((char *)page + (nFields + 2) * sizeof(int), data, recordLength);

        //Slot table
        int size = 1;
        memcpy((char *)page + PAGE_SIZE - sizeof(int), &size, sizeof(int));
        size = sizeof(int);
        memcpy((char *)page + PAGE_SIZE - 2 * sizeof(int), &size, sizeof(int));
        memcpy(&size, (char *)page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
        fileHandle.appendPage(page);       
    }
    else
    {
#ifdef DEBUG
        printf("[insertRecord] write existing page\n");
#endif
        int nSlot;
        int insertSlot;
        //Get total number of slots
        memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
        int offset;
        int lastLength;
        int i;
        for (i = 0; i < nSlot; ++i)
        {
            // printf("[insertRecord] check slot %d\n", i);
            memcpy(&offset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
            if (offset == -1)
                break;
        }
        if (i == 0)
        {
            offset = 0;
            lastLength = sizeof(int);
            insertSlot = 0;
        }
        else
        {
            memcpy(&offset, (char *)page + PAGE_SIZE - (i + 1) * sizeof(int), sizeof(int));
            offset = abs(offset);
            memcpy(&lastLength, (char *)page + offset, sizeof(int));
            if (lastLength == -1)
            //This is a stump
            {
                lastLength = 3 * sizeof(int);
            }
            insertSlot = i;
        }
        
        offset += lastLength;
        rid.slotNum = insertSlot;
        recordLength += (nFields + 1) * sizeof(int);
#ifdef DEBUG
        printf("[insertRecord] insert offset: %d, insert recordLength: %d, insert slotNum: %d\n", offset, recordLength, insertSlot);
#endif
        if (insertSlot < nSlot - 1)
        {
            moveRecords(recordLength, page, insertSlot + 1, RIGHT);
        }
        for (int i = 0; i < nFields; ++i)
        {
            // printf("[insertRecord: write field %d\n", i);
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        // total += nFields * sizeof(int);
        memcpy((char *)page + offset, &recordLength, sizeof(int));
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength - (nFields + 1) * sizeof(int));

        total += (nSlot == insertSlot ? sizeof(int) : 0) + recordLength;
        memcpy((char *)page, &total, sizeof(int));

        //Slot table
        int size = nSlot == insertSlot ? nSlot + 1 : nSlot;
        memcpy((char *)page + PAGE_SIZE - sizeof(int), &size, sizeof(int));
        memcpy((char *)page + PAGE_SIZE - (insertSlot + 2) * sizeof(int), &offset, sizeof(int));
        fileHandle.writePage(cPage, page);        
    }
    rid.pageNum = cPage;


    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (rid.slotNum + 2) * sizeof(int), sizeof(int));
    offset = abs(offset);
#ifdef DEBUG
    printf("[insertRecord] pageNum: %d, slotNum: %d, length: %d, offset: %d\n\n", rid.pageNum, rid.slotNum, recordLength, offset);
#endif
    free(page);
    return 0;    
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    int cPage = rid.pageNum;
    int nFields = recordDescriptor.size();
    int recordLength;
    int slotNum = rid.slotNum;
    // printf("pageNum: %d, slotNum: %d\n", rid.pageNum, rid.slotNum);
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);

    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
    if (offset == -1)
    {
#ifdef DEBUG
        printf("[readRecord] This record (%d, %d) has been deleted\n", cPage, slotNum);
#endif
        return -1;
    }
    offset = abs(offset);

    memcpy(&recordLength, (char *)page + offset, sizeof(int));
#ifdef DEBUG
    printf("[readRecord] pageNum: %d, slotNum: %d, offset: %d\n", rid.pageNum, rid.slotNum, offset);
#endif
    while (recordLength == -1)
    {
        
        memcpy(&cPage, (char *)page + offset + sizeof(int), sizeof(int));
        memcpy(&slotNum, (char *)page + offset + 2 * sizeof(int), sizeof(int));
        fileHandle.readPage(cPage, page);
        // int offset;
        memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
        offset = abs(offset);
        memcpy(&recordLength, (char *)page + offset, sizeof(int));
#ifdef DEBUG
        printf("[readRecord] stump points to (%d, %d); recordLength: %d, offset: %d\n", 
            cPage, slotNum, recordLength, offset);
#endif
    }
    // printf("recordlength: %d\n", recordLength);
#ifdef DEBUG
        printf("[readRecord] read from (%d, %d); recordLength: %d, offset: %d\n", 
            cPage, slotNum, recordLength, offset);
#endif
    memcpy(data, (char *)page + offset + (nFields + 1) * sizeof(int), recordLength - (nFields + 1) * sizeof(int));
    // printf("record length: %d\n", recordLength);
    // this->printRecord(recordDescriptor, data);
    free(page);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // printf("Size of data: %d, data: %c\n", sizeof(data), ((char *)data)[0]);
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
// #ifdef DEBUG
//     for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
//     {
//         printf("[printRecord] %d\n", nullFieldsIndicator[i]);
//     }
//     printf("\n");
// #endif
    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    for (int i = 0; i < nFields; ++i)
    {
        int nByte = i / 8;
        int nBit = i % 8;
        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
        if(!nullBit)
        {
            if (recordDescriptor[i].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
                printf("%s: %-10d\t", recordDescriptor[i].name.c_str(), value);
            }
            if (recordDescriptor[i].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
                printf("%s: %-10f\t", recordDescriptor[i].name.c_str(), value);
            }
            if (recordDescriptor[i].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                char* value = (char *) malloc(nameLength + 1);
                memcpy(value, (char *)data + offset, nameLength);
                value[nameLength] = '\0';
                offset += nameLength;
                printf("%s: %-10s\t", recordDescriptor[i].name.c_str(), value);
            }          
        }
        else
        {
            printf("%s: NULL\t", recordDescriptor[i].name.c_str());
        }
    }
    printf("\n");
    free(nullFieldsIndicator);
    return 0;
}

RC RecordBasedFileManager::getRecordLength(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // printf("Size of data: %d, data: %c\n", sizeof(data), ((char *)data)[0]);
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    for (int i = 0; i < nFields; ++i)
    {
        int nByte = i / 8;
        int nBit = i % 8;
        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
        if(!nullBit)
        {
            if (recordDescriptor[i].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                char* value = (char *) malloc(nameLength);
                memcpy(value, (char *)data + offset, nameLength);
                offset += nameLength;
            }          
        }
    }
    return offset;
}

RC RecordBasedFileManager::getIndexList(const vector<Attribute> &recordDescriptor, const void *data, int *indexList)
{
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    for (int i = 0; i < nFields; ++i)
    {
        int nByte = i / 8;
        int nBit = i % 8;
        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
        if(!nullBit)
        {
            indexList[i] = offset;
            if (recordDescriptor[i].type == TypeInt)
            {
                int value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeReal)
            {
                float value;
                memcpy(&value, (char *)data + offset, recordDescriptor[i].length);
                offset += recordDescriptor[i].length;
            }
            if (recordDescriptor[i].type == TypeVarChar)
            {
                int nameLength;
                memcpy(&nameLength, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                // printf("String length: %d\n", nameLength);
                char* value = (char *) malloc(nameLength);
                memcpy(value, (char *)data + offset, nameLength);
                offset += nameLength;
            }          
        }
        else
        {
            indexList[i] = -1;
        }
    }
    // for (int i = 0; i < nFields; ++i)
    // {
    //     printf("%d ", indexList[i]);
    // }
    // printf("\n");
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
    int cPage = rid.pageNum;
    int nFields = recordDescriptor.size();
    int recordLength;
    int nSlot;
    int slotNum = rid.slotNum;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);
    int total;
    memcpy(&total, (char *)page, sizeof(int));   

    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    if (offset == -1)
        return -1;
    offset = abs(offset);
    memcpy(&recordLength, (char *)page + offset, sizeof(int));
    // printf("recordLength %d\n", recordLength);

    // for (int i = slotNum + 1; i < nSlot; ++i)
    // {
    //     void *data = malloc(PAGE_SIZE);
    //     int oldOffset;
    //     memcpy(&oldOffset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
    //     int oldRecordLength;
    //     memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
    //     memcpy(data, (char *)page +  oldOffset, oldRecordLength);

    //     memcpy((char *)page + offset, data, oldRecordLength);
    //     memcpy((char *)page + PAGE_SIZE - (i + 2) * sizeof(int), &offset, sizeof(int));
    //     offset += oldRecordLength;
    //     free(data);
    // }
    if (recordLength == -1)
    {
        RID newRid;
        memcpy(&newRid.pageNum, (char *)page + offset + sizeof(int), sizeof(int));
        memcpy(&newRid.slotNum, (char *)page + offset + 2 * sizeof(int), sizeof(int));
#ifdef DEBUG
    printf("[deleteRecord] This is a stump, need to delete recursively. cPage: %d, pageNum: %d, slotNum: %d, offset: %d\n", 
        cPage, newRid.pageNum, newRid.slotNum, offset);
#endif
        this->deleteRecord(fileHandle, recordDescriptor, newRid);
    }
#ifdef DEBUG
    printf("[deleteRecord] Moving record left, pageNum: %d, offset: %d, from slotNum: %d\n", cPage, offset, slotNum + 1);
#endif
    this->moveRecords(offset, page, slotNum + 1, LEFT);

    int size = -1;
    memcpy((char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), &size, sizeof(int));
    total -= recordLength == -1 ? 3 * sizeof(int) : recordLength;
    memcpy((char *)page, &total, sizeof(int));
#ifdef DEBUG
    printf("[deleteRecord] offset: %d, nSlot: %d, pageNum: %d, slotNum: %d, recordLength: %d, total before: %d, total after: %d\n", 
        offset, nSlot, cPage, slotNum, recordLength, total + (recordLength == -1 ? 3 * sizeof(int) : recordLength), total);
#endif
    fileHandle.writePage(cPage, page);  
    free(page);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    int nFields = recordDescriptor.size();
    int cPage = rid.pageNum;
    int slotNum = rid.slotNum;
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(cPage, page);

    int nSlot;
    //Get total number of slots
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));

#ifdef DEBUG
    printf("[updateRecord] cPage: %d, slotNum: %d, total pages: %d, total slots: %d\n", cPage, slotNum, fileHandle.getNumberOfPages(), nSlot);
#endif
    int total;
    int *indexList = new int[nFields];
    this->getIndexList(recordDescriptor, data, indexList);

    memcpy(&total, (char *)page, sizeof(int));
    int recordLength = this->getRecordLength(recordDescriptor, data);
    int left = PAGE_SIZE - total;
    int oldRecordLength;
    int offset;
    memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
    offset = abs(offset);
    memcpy(&oldRecordLength, (char *)page + offset, sizeof(int));

    if (recordLength == -1)
    {
        int nPage;
        int nSlotNum;
        memcpy(&nPage, (char *)page + offset + sizeof(int), sizeof(int));
        memcpy(&nSlotNum, (char *)page + offset + 2 * sizeof(int), sizeof(int));
        fileHandle.readPage(nPage, page);
        oldRecordLength = 3 * sizeof(int);
        RID nRid;
        nRid.pageNum = nPage;
        nRid.slotNum = nSlotNum;
        this->deleteRecord(fileHandle, recordDescriptor, nRid);
        // int offset;
        // memcpy(&offset, (char *)page + PAGE_SIZE - (nSlotNum + 2) * sizeof(int), sizeof(int));
        // offset = abs(offset);
        // memcpy(&recordLength, (char *)page + offset, sizeof(int));
#ifdef DEBUG
        printf("[updateRecord] stump points to (%d, %d)\n", 
            nPage, nSlotNum);
#endif
    }

#ifdef DEBUG
    printf("[updateRecord] record offset: %d, old record length: %d, new record length:%d, total: %d, left: %d\n", 
        offset, oldRecordLength, recordLength + (nFields + 1) * sizeof(int), total, left);
#endif
    if (recordLength + (nFields + 1) * sizeof(int) == oldRecordLength)
    {
#ifdef DEBUG
        printf("[updateRecord] New length is equal to old length\n");
#endif
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength);
        fileHandle.writePage(cPage, page);   
    } else if (recordLength + (nFields + 1) * sizeof(int) < oldRecordLength)
    {
#ifdef DEBUG
        printf("[updateRecord] New length is smaller than old length\n");
#endif
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        recordLength += (nFields + 1) * sizeof(int);
        memcpy((char *)page + offset, &recordLength, sizeof(int));
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength - (nFields + 1) * sizeof(int));

        total = total + (recordLength - oldRecordLength);
        memcpy((char *)page, &total, sizeof(int));

        offset += recordLength;

        this->moveRecords(offset, page, slotNum + 1, LEFT);
        fileHandle.writePage(cPage, page);   
    } else if (recordLength + (nFields + 1) * sizeof(int) - oldRecordLength > left)
    {
        RID tempRID;
        this->insertRecord(fileHandle, recordDescriptor, data, tempRID);
#ifdef DEBUG
        printf("[updateRecord] New record exceeds the maximum of current page.\n New pageNum: %d, new slotNum: %d\n",
            tempRID.pageNum, tempRID.slotNum);
#endif
        int size = -1;
        memcpy((char *)page + offset, &size, sizeof(int));
        // printf("offset: %d\n", offset);
        if (oldRecordLength >= 3 * sizeof(int))
        {
            memcpy((char *)page + offset + sizeof(int), &tempRID.pageNum, sizeof(int));
            memcpy((char *)page + offset + 2 * sizeof(int), &tempRID.slotNum, sizeof(int));
            this->moveRecords(offset + 3 * sizeof(int), page, slotNum + 1, LEFT);
        } else
        {
            this->moveRecords(3 * sizeof(int) - oldRecordLength, page, slotNum + 1, RIGHT);
            memcpy((char *)page + offset + sizeof(int), &tempRID.pageNum, sizeof(int));
            memcpy((char *)page + offset + 2 * sizeof(int), &tempRID.slotNum, sizeof(int));
        }
        total = total + 3 * sizeof(int) - oldRecordLength;
        memcpy((char *)page, &total, sizeof(int));
        fileHandle.writePage(cPage, page);   
#ifdef DEBUG
        printf("[updateRecord] Written stump on cPage: %d\n", cPage);
#endif

        //Mark this as a record which is pointed to by a stump
        int tempOffset;
        int RC = fileHandle.readPage(tempRID.pageNum, page);
        if (RC != 0)
        {
#ifdef DEBUG
            printf("[updateRecord] Error when read page %d", tempRID.pageNum);
#endif  
        }

        memcpy(&tempOffset, (char *)page + PAGE_SIZE - (tempRID.slotNum + 2) * sizeof(int), sizeof(int));
#ifdef DEBUG
        printf("[updateRecord] Written stump, (%d, %d)\n", tempRID.pageNum, tempRID.slotNum);
        printf("[updateRecord] Written stump, tempOffset: %d\n", tempOffset);
#endif
        if (tempRID.pageNum == 0 && tempRID.slotNum == 44){
            printf("Error!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n!!!\n!!!!\n!!!!\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
        tempOffset = -tempOffset;
        memcpy((char *)page + PAGE_SIZE - (tempRID.slotNum + 2) * sizeof(int), &tempOffset, sizeof(int));
        RC = fileHandle.writePage(tempRID.pageNum, page);

        if (RC != 0)
        {
#ifdef DEBUG
            printf("[updateRecord] Error when read page %d", tempRID.pageNum);
#endif  
        }
        // printf("cPage: %d, offset: %d, (%d, %d)\n", cPage, offset, pageNum, slotNum);
    } else
    {
#ifdef DEBUG
        printf("[updateRecord] New record can still be placed within this page.\n");
#endif
        this->moveRecords(recordLength + (nFields + 1) * sizeof(int) - oldRecordLength, page, slotNum + 1, RIGHT);
        for (int i = 0; i < nFields; ++i)
        {
            memcpy((char *)page + offset + (i + 1) * sizeof(int), &indexList[i], sizeof(int));
        }
        recordLength += (nFields + 1) * sizeof(int);
        memcpy((char *)page + offset, &recordLength, sizeof(int));
        memcpy((char *)page + offset + (nFields + 1) * sizeof(int), data, recordLength - (nFields + 1) * sizeof(int));

        // printf("[updateRecord] new: %d, old: %d, extra: %d\n", 
        //     recordLength,  oldRecordLength, (recordLength - oldRecordLength));
        total = total + (recordLength - oldRecordLength);
        memcpy((char *)page, &total, sizeof(int));

        offset += recordLength;
        fileHandle.writePage(cPage, page);   
    }
    free(page);
    return 0;
}

RC RecordBasedFileManager::moveRecords(int offset, void *page, int slotNum, Direction direction)
{
    //If move left, then offset represents the start of the first move,
    //If move right, then offset represents the right shift of the data.

    int nSlot;
    //Get total number of slots
    memcpy(&nSlot, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    if (direction == LEFT)
    {
        for (int i = slotNum; i < nSlot; ++i)
        {
            void *data = malloc(PAGE_SIZE);
            int oldOffset;
            memcpy(&oldOffset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
            if (oldOffset == -1)
                continue;
            int sign = oldOffset > 0 ? 1 : -1;
#ifdef DEBUG
            printf("[moveLeft] old offset: %d, slotNum: %d\n", oldOffset, i);
#endif
            oldOffset = abs(oldOffset);
            int oldRecordLength;
            memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
            if (oldRecordLength == -1)
            //This is a stump
                oldRecordLength = 3 * sizeof(int);
            memcpy(data, (char *)page +  oldOffset, oldRecordLength);

            memcpy((char *)page + offset, data, oldRecordLength);
            offset *= sign;
#ifdef DEBUG
            printf("[moveLeft] new offset: %d, slotNum %d\n", offset, i);
#endif
            memcpy((char *)page + PAGE_SIZE - (i + 2) * sizeof(int), &offset, sizeof(int));
            offset = abs(offset);
            offset += oldRecordLength;
            free(data);
        }
    } else if (direction == RIGHT)
    {
        int shift = offset;
        for (int i = nSlot - 1; i >= slotNum; --i)
        {
            void *data = malloc(PAGE_SIZE);
            int oldOffset;
            memcpy(&oldOffset, (char *)page + PAGE_SIZE - (i + 2) * sizeof(int), sizeof(int));
            if (oldOffset == -1)
                continue;
            int sign = oldOffset > 0 ? 1 : -1;
#ifdef DEBUG
            printf("[moveRight] old offset: %d, slotNum: %d\n", oldOffset, i);
#endif
            oldOffset = abs(oldOffset);
            int oldRecordLength;
            memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
            if (oldRecordLength == -1)
            //This is a stump
                oldRecordLength = 3 * sizeof(int);

            memcpy(&oldRecordLength, (char *)page +  oldOffset, sizeof(int));
            memcpy(data, (char *)page +  oldOffset, oldRecordLength);

            int newOffset = oldOffset + shift;
#ifdef DEBUG
            printf("[moveRecords] %d, oldOffset: %d, new offset: %d\n", i, oldOffset, newOffset);
#endif
            // offset = oldOffset + shift;
            
            memcpy((char *)page + newOffset, data, oldRecordLength);
            newOffset *= sign;
#ifdef DEBUG
            printf("[moveRight] new offset: %d, slotNum %d\n", newOffset, i);
#endif
            memcpy((char *)page + PAGE_SIZE - (i + 2) * sizeof(int), &newOffset, sizeof(int));
            free(data);   
        }
    }
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
    int nFields = recordDescriptor.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    void* content = malloc(PAGE_SIZE);
    this->readRecord(fileHandle, recordDescriptor, rid, content);

    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)content)[i];
    }
    int offset = nullFieldsIndicatorActualSize;
    // printf("Number of fields: %d, actual field size: %d, %d\n", nFields, nullFieldsIndicatorActualSize, nullFieldsIndicator[0]);
    int i;
    for (i = 0; i < nFields; ++i)
    {
        if (recordDescriptor[i].name == attributeName)
            break;
    }

    //Not found the attribute
    if (i == nFields)
        return -1;

    int nByte = i / 8;
    int nBit = i % 8;
    bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
    int returnDataOffset = 1;
    int isNull = nullBit ? 1 : 0;
    memcpy(data, &isNull, recordDescriptor[i].length);

    if(!nullBit)
    {
        int cPage = rid.pageNum;
        int slotNum = rid.slotNum;
        void *page = malloc(PAGE_SIZE);
        int recordLength;
        fileHandle.readPage(cPage, page);

        memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
        if (offset == -1)
        {
#ifdef DEBUG
            printf("[readAttribute] This record (%d, %d) has been deleted\n", cPage, slotNum);
#endif
            return -1;
        }
        offset = abs(offset);

        memcpy(&recordLength, (char *)page + offset, sizeof(int));
        // printf("pageNum: %d, slotNum: %d\n", rid.pageNum, rid.slotNum);
        while (recordLength == -1)
        {
            memcpy(&cPage, (char *)page + offset + sizeof(int), sizeof(int));
            memcpy(&slotNum, (char *)page + offset + 2 * sizeof(int), sizeof(int));
            fileHandle.readPage(cPage, page);
            // int offset;
            memcpy(&offset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
            offset = abs(offset);
            memcpy(&recordLength, (char *)page + offset, sizeof(int));
#ifdef DEBUG
            printf("[readAttribute] stump points to (%d, %d); recordLength: %d, offset: %d\n", 
                cPage, slotNum, recordLength, offset);
#endif
        }

        int recordOffset;
        memcpy(&recordOffset, (char *)page + PAGE_SIZE - (slotNum + 2) * sizeof(int), sizeof(int));
        if (recordOffset == -1)
        {
#ifdef DEBUG
        printf("[readAttribute] This record (%d, %d) has been deleted\n", cPage, slotNum);
#endif
            return -1;
        }
        
        recordOffset = abs(recordOffset);
        memcpy(&offset, (char *)page + recordOffset + (1 + i) * sizeof(int), sizeof(int));

#ifdef DEBUG
        printf("[readAttribute] record offset: %d, attribute offset: %d, i: %d\n", recordOffset, offset, i);
#endif
        if (recordDescriptor[i].type == TypeInt)
        {
            memcpy((char *)data + returnDataOffset, (char *)content + offset, recordDescriptor[i].length);
        }
        if (recordDescriptor[i].type == TypeReal)
        {
            memcpy((char *)data + returnDataOffset, (char *)content + offset, recordDescriptor[i].length);
        }
        if (recordDescriptor[i].type == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)content + offset, sizeof(int));
            // printf("String length: %d\n", nameLength);
            memcpy((char *)data + returnDataOffset, &nameLength, sizeof(int));
            returnDataOffset += sizeof(int);
            memcpy((char *)data + returnDataOffset, (char *)content + offset + sizeof(int), nameLength);
        }          
        free(page);
    }
    else
    {
        unsigned int d = 1 << 7;
        memcpy(data, &d, sizeof(unsigned int));
    }
    free(content);
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
  const vector<Attribute> &recordDescriptor,
  const string &conditionAttribute,
  const CompOp compOp,                  // comparision type such as "<" and "="
  const void *value,                    // used in the comparison
  const vector<string> &attributeNames, // a list of projected attributes
  RBFM_ScanIterator &rbfm_ScanIterator)
{
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = value;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
    rbfm_ScanIterator.attributePositions = new int[attributeNames.size()];
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.attributeNames = attributeNames;

    for (int i = 0; i < attributeNames.size(); ++i)
    {
        int j;
        for (j = 0; i < recordDescriptor.size(); ++j)
        {
            if (recordDescriptor[j].name == attributeNames[i])
            {
                rbfm_ScanIterator.attributePositions[i] = j;
                break;
            }
        }
        if (j == recordDescriptor.size())
        {
#ifdef DEBUG
            printf("[rbfm::scan] Cannot find attributes Id\n");
#endif
            return -1;
        }
    }
    int i;
    for (i = 0; i < recordDescriptor.size(); ++i)
    {
        if (recordDescriptor[i].name == conditionAttribute)
        {
            rbfm_ScanIterator.conditionAttributePosition = i;
            break;
        }
    }
    if (i == recordDescriptor.size())
    {
        if (compOp != NO_OP)
        {
#ifdef DEBUG
            printf("[rbfm::scan] Cannot find conditionAttributePosition\n");
#endif
            return -1;
        }   
        else
        {
#ifdef DEBUG
            printf("[rbfm::scan] No op!\n");
#endif 
            rbfm_ScanIterator.conditionAttributePosition = -1;
        }
    }
    return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
    cPage = 0;
    cSlot = -1;
}

RC RBFM_ScanIterator::close()
{
    cPage = 0;
    cSlot = -1;
    return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
    if (fileHandle->getNumberOfPages() <= 0)
        return RBFM_EOF;
    void * page = malloc(PAGE_SIZE);
    fileHandle->readPage(cPage, page);
    int nSlots;
    memcpy(&nSlots, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
    bool isStump = false;

    while (1)
    {
        if(isStump)
            fileHandle->readPage(cPage, page);
        
        cSlot++;
#ifdef DEBUG
        printf("[getNextRecord] current page: %d, current slot:%d, total pages: %d, total slots: %d\n", cPage, cSlot, fileHandle->getNumberOfPages(), nSlots);
#endif
        if (cSlot > nSlots - 1)
        {
            cPage++;
            cSlot = 0;
            fileHandle->readPage(cPage, page);
            memcpy(&nSlots, (char *)page + PAGE_SIZE - sizeof(int), sizeof(int));
        }
        if (cPage > fileHandle->getNumberOfPages() - 1)
        {
            free(page);
            return RBFM_EOF;
        }

        int offset;
        
        bool valid = true;
        bool satisfied = false;
        memcpy(&offset, (char *)page + PAGE_SIZE - (cSlot + 2) * sizeof(int), sizeof(int));

        if (offset < 0)
            valid = false;
        rid.slotNum = cSlot;
        rid.pageNum = cPage;
#ifdef DEBUG
        printf("[getNextRecord] offset: %d\n", offset);
#endif

        if (valid)
        {
            int offset;
            memcpy(&offset, (char *)page + PAGE_SIZE - (cSlot + 2) * sizeof(int), sizeof(int));
            offset = abs(offset);

            //Has been deleted
            if (offset == -1)
            {
                free(page);
                return 1;
            }
            int recordLength;
            memcpy(&recordLength, (char *)page + offset, sizeof(int));

            isStump  = false;
            //Update to new place
            while (recordLength == -1)
            {
                int tPage;
                int tSlot;
                isStump = true;
                memcpy(&tPage, (char *)page + offset + sizeof(int), sizeof(int));
                memcpy(&tSlot, (char *)page + offset + 2 * sizeof(int), sizeof(int));
                fileHandle->readPage(tPage, page);
                memcpy(&offset, (char *)page + PAGE_SIZE - (tSlot + 2) * sizeof(int), sizeof(int));
                offset = abs(offset);
                memcpy(&recordLength, (char *)page + offset, sizeof(int));
#ifdef DEBUG
            printf("[getNextRecord] Stump found, pointing to (%d, %d)\n", tPage, tSlot);
#endif
            }
            int nFields = recordDescriptor.size();
            int dataOffset = offset + (nFields + 1) * sizeof(int);
            int attrOffset;
            bool isNull = false;
            memcpy(&attrOffset, (char *)page + offset + (conditionAttributePosition + 1) * sizeof(int), sizeof(int));
            if (attrOffset == -1)
            {
                satisfied = false;
                isNull = true;
            }
#ifdef DEBUG
            printf("[getNextRecord] offset: %d, dataOffset: %d, attrOffset: %d, recordLength: %d, conditionAttributePosition: %d\n",
                offset, dataOffset, attrOffset, recordLength, conditionAttributePosition);
#endif
            if (compOp == NO_OP)
            {
                satisfied = true;
            }
            else if (!isNull)
            {
                if (recordDescriptor[conditionAttributePosition].type == TypeInt)
                {
                    int value;
                    memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[conditionAttributePosition].length);
                    int searchValue;
                    memcpy(&searchValue, this->value, recordDescriptor[conditionAttributePosition].length);
#ifdef DEBUG
                    // printf("[getNextRecord] value: %d, searchValue: %d\n", value, searchValue);
#endif
                    
                    switch (compOp)
                    {
                        case EQ_OP:
                            satisfied = value == searchValue;
                            break;
                        case LT_OP:
                            satisfied = value < searchValue;
                            break;
                        case LE_OP:
                            satisfied = value <= searchValue;
                            break;
                        case GT_OP:
                            satisfied = value > searchValue;
                            break;
                        case GE_OP:
                            satisfied = value >= searchValue;
                            break;
                        case NE_OP:
                            satisfied = value != searchValue;
                            break;
                        case NO_OP:
                            satisfied = true;
                            break;
                    }
                }
                if (recordDescriptor[conditionAttributePosition].type == TypeReal)
                {
                    float value;
                    memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[conditionAttributePosition].length);
                    float searchValue;
                    memcpy(&searchValue, this->value, recordDescriptor[conditionAttributePosition].length);
                    
                    switch (compOp)
                    {
                        case EQ_OP:
                            satisfied = value == searchValue;
                            break;
                        case LT_OP:
                            satisfied = value < searchValue;
                            break;
                        case LE_OP:
                            satisfied = value <= searchValue;
                            break;
                        case GT_OP:
                            satisfied = value > searchValue;
                            break;
                        case GE_OP:
                            satisfied = value >= searchValue;
                            break;
                        case NE_OP:
                            satisfied = value != searchValue;
                            break;
                        case NO_OP:
                            satisfied = true;
                            break;
                    }
                }
                if (recordDescriptor[conditionAttributePosition].type == TypeVarChar)
                {
                    int nameLength;
                    memcpy(&nameLength, (char *)page + dataOffset + attrOffset, sizeof(int));
                    char* value_c = (char *) malloc(nameLength + 1);
                    memcpy(value_c, (char *)page + dataOffset + attrOffset + sizeof(int), nameLength);
                    value_c[nameLength] = '\0';

                    int searchValueLength;
                    memcpy(&searchValueLength, (char *)this->value, sizeof(int));
                    char* value_s = (char *) malloc(searchValueLength + 1);
                    memcpy(value_s, (char *) this->value + sizeof(int), searchValueLength);
                    value_s[searchValueLength] = '\0';

                    string searchValue = string(value_s);
                    string value = string(value_c);
#ifdef DEBUG
                    printf("[getNextRecord] name length: %d, value: %s, searchValue: %s, searchValueLength: %d\n", nameLength, value.c_str(), searchValue.c_str(), searchValueLength);
#endif

                    switch (compOp)
                    {
                        case EQ_OP:
                            satisfied = value == searchValue;
                            break;
                        case LT_OP:
                            satisfied = value < searchValue;
                            break;
                        case LE_OP:
                            satisfied = value <= searchValue;
                            break;
                        case GT_OP:
                            satisfied = value > searchValue;
                            break;
                        case GE_OP:
                            satisfied = value >= searchValue;
                            break;
                        case NE_OP:
                            satisfied = value != searchValue;
                            break;
                        case NO_OP:
                            satisfied = true;
                            break;
                    }
            }
        }

            if (satisfied)
            {
#ifdef DEBUG
                // printf("[getNextRecord] Satisfied!\n");
#endif
                int nFields = attributeNames.size();
                int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
                unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
                int returnDataOffset = nullFieldsIndicatorActualSize;
                memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
                for (int i = 0; i < attributeNames.size(); ++i)
                {
                    memcpy(&attrOffset, (char *)page + offset + (attributePositions[i] + 1) * sizeof(int), sizeof(int));
                    if (attrOffset == -1)
                    {
                        int nByte = i / 8;
                        int nBit = i % 8;
                        nullsIndicator[nByte] |= (1 << (7 - nBit));
                        // bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit)); 
                    } else
                    {
                        if (recordDescriptor[attributePositions[i]].type == TypeInt)
                        {
                            int value;
                            memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[attributePositions[i]].length);
                            memcpy((char *)data + returnDataOffset, &value, recordDescriptor[attributePositions[i]].length);
                            returnDataOffset += recordDescriptor[attributePositions[i]].length;
                        }
                        if (recordDescriptor[attributePositions[i]].type == TypeReal)
                        {
                            float value;
                            memcpy(&value, (char *)page + dataOffset + attrOffset, recordDescriptor[attributePositions[i]].length);
                            memcpy((char *)data + returnDataOffset, &value, recordDescriptor[attributePositions[i]].length);
                            returnDataOffset += recordDescriptor[attributePositions[i]].length;
                        }
                        if (recordDescriptor[attributePositions[i]].type == TypeVarChar)
                        {
                            int nameLength;
                            memcpy(&nameLength, (char *)page + dataOffset + attrOffset, sizeof(int));
                            memcpy((char *)data + returnDataOffset, &nameLength, sizeof(int));
                            returnDataOffset += sizeof(int);
                            
                            char* value = (char *) malloc(nameLength);
                            memcpy(value, (char *)page + dataOffset + attrOffset + sizeof(int), nameLength);
                            memcpy((char *)data + returnDataOffset, value, nameLength);
                            returnDataOffset += nameLength;
                        }   
                    }
                }
                memcpy((char *)data, nullsIndicator, nullFieldsIndicatorActualSize);
                free(page);
                return 0;
            }
        }
    }
    free(page);
}
