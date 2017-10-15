#include "pfm.h"
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <string>
using namespace std;

bool fexists(const char *filename)
{
    ifstream ifile(filename);
    return (bool)ifile;
}

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    if(fexists(fileName.c_str()))
        return 1;
    fstream fs;
    fs.open(fileName, ios::out);
    fs.close();
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if (!fexists(fileName.c_str()))
    {
        return 1;
    }
    remove(fileName.c_str());
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return fileHandle.openFile(fileName);
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return fileHandle.closeFile();
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    nPages = 0;
}

RC FileHandle::openFile(const string &fileName)
{
    try
    {
        fs.open(fileName);
    }
    catch (fstream::failure e)
    {
        cerr << "Exception in open/reading/closing file\n";
        return 1;
    }
    return 0;
}

RC FileHandle::closeFile()
{
    if (fs)
    {
        try
        {
            fs.close();
        }
        catch (fstream::failure e)
        {
            cerr << "Exception in open/reading/closing file\n";
            return 1;
        }
        return 0;
    }
    return 2;
}

FileHandle::~FileHandle()
{
    this->closeFile();
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (pageNum >= this->getNumberOfPages())
    {
        return 1;
    }
    fs.seekg(pageNum*PAGE_SIZE);
    fs.read((char *)data, PAGE_SIZE);
    this->readPageCounter++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (pageNum >= this->getNumberOfPages())
    {
        return 1;
    }
    fs.seekp(pageNum*PAGE_SIZE);
    fs.write((char *)data, PAGE_SIZE);
    this->writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    fs.seekp(0, fs.end);
    fs.write((char *)data, PAGE_SIZE);
    this->appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    fs.seekg(0, fs.end);
    int length = fs.tellg();
    return length / PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
