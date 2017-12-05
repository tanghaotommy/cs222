
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) 
{
	this->input = input;
	this->condition = &condition;
	input->getAttributes(this->attrs);

	for (int i = 0; i < this->attrs.size(); ++i)
	{
// 		vector<string> strs;
// 		split(strs, this->attrs[i].name);
// 		if (strs.size() != 2)
// 		{
// #ifdef DEBUG
// 			printf("[Filter] Initiliazation failed because of invalid split, split size: %d\n", strs.size());
// #endif
// 		}
// 		string attributeName = strs[this->attrs.size() - 1];
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->attrs[i].name = attributeName;
	}
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
    attrs.clear();
    attrs = this->attrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = "Filter";
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
}


RC Filter::getNextTuple(void *data)
{
	while (this->input->getNextTuple(data) != RM_EOF)
	{
#ifdef DEBUG_QE
		RelationManager::instance()->printTuple(this->attrs, data);
#endif

		if (this->condition->bRhsIsAttr)
		{
#ifdef DEBUG_QE
			printf("[Filter::getNextTuple]Right hand is an attribute\n");
			return QE_EOF;
#endif
		}
		else
		{
			void *value = malloc(PAGE_SIZE);
			int pos = getValueOfAttrByName(data, this->attrs, getOriginalAttrName(this->condition->lhsAttr), value);

#ifdef DEBUG_QE
			int v;
			memcpy(&v, value, sizeof(int));
			printf("[Filter::getNextTuple] return value from getValueOfAttrByName: %d\n", v);
#endif


			if (pos == -1 || pos == -2)
			{
#ifdef DEBUG_QE
				printf("[Filter::getNextTuple] position of the attribute: %d\n", pos);
#endif
				free(value);
				continue;
			}

			bool satisfied = compareCondition(&this->attrs[pos], value, this->condition);
			free(value);
			if (satisfied)
				return 0;
		}
	}
	return QE_EOF;
}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->input = input;
	input->getAttributes(this->attrs);

	for (int i = 0; i < this->attrs.size(); ++i)
	{
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->attrs[i].name = attributeName;
	}

	for (int i = 0; i < attrNames.size(); ++i)
	{
		int pos = attrNames[i].find('.');
		string attributeName = attrNames[i].substr(pos + 1, attrNames[i].length() - pos + 1);

		for (int j = 0; j < this->attrs.size(); ++j)
		{
			if (attributeName == this->attrs[j].name)
			{
				this->projectAttrs.push_back(this->attrs[j]);
				break;
			}
		}
	}
	if (this->projectAttrs.size() != attrNames.size())
	{
#ifdef DEBUG_QE
		printf("[Project::Project] Some attrNames can not be found!\n");
#endif
	}
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = this->projectAttrs;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = "Project";
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }
}

RC Project::getNextTuple(void *data)
{
	void* temp = malloc(PAGE_SIZE);
	if (this->input->getNextTuple(temp) != RM_EOF)
	{
#ifdef DEBUG_QE
		RelationManager::instance()->printTuple(this->attrs, temp);
#endif
		int nFields = this->projectAttrs.size();
	    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
	    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);

	    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
	    {
	        nullFieldsIndicator[i] = 0;
	    }
	    int offset = nullFieldsIndicatorActualSize;
	    for (int i = 0; i < this->projectAttrs.size(); ++i)
	    {
	    	int nByte = i / 8;
    		int nBit = i % 8;
    		void *value = malloc(PAGE_SIZE);
    		int rc = getValueOfAttrByName(temp, this->attrs, this->projectAttrs[i].name, value);
    		if (rc == -1)
    		{
    			nullFieldsIndicator[nByte] |= (1 << (7 - nBit));
    		}
    		else
    		{
    			if (this->projectAttrs[i].type == TypeInt)
	            {
	            	// int v;
	                memcpy((char *)data + offset, value, this->projectAttrs[i].length);
	                // memcpy(&v, value, sizeof(int));
	                offset += this->projectAttrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10d\n", attrs[i].name.c_str(), value);
	            }
	            if (this->projectAttrs[i].type == TypeReal)
	            {
	                memcpy((char *)data + offset, value, this->projectAttrs[i].length);
	                offset += this->projectAttrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10f\n", attrs[i].name.c_str(), value);
	            }
	            if (this->projectAttrs[i].type == TypeVarChar)
	            {
	                int nameLength;
	                memcpy(&nameLength, value, sizeof(int));
	                memcpy((char *)data + offset, value, sizeof(int));
	                offset += sizeof(int);
	                // printf("String length: %d\n", nameLength);
	                memcpy((char *)data + offset, (char *)value + sizeof(int), nameLength);
	                offset += nameLength;
	                //printf("%s: %-10s\t", attrs[i].name.c_str(), value);
	            }      
    		}
    		free(value);
	    }
	    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	    free(nullFieldsIndicator);
	    free(temp);
	    return 0;
	} else
	{
		free(temp);
		return QE_EOF;
	}
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
	this->input = input;
	this->hasGroupBy = false;
	input->getAttributes(this->attrs);
	this->op = op;
	this->aggAttr = aggAttr;
	this->total = 1;

	for (int i = 0; i < this->attrs.size(); ++i)
	{
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->relation = attrs[i].name.substr(0, pos);
		this->attrs[i].name = attributeName;
	}
#ifdef DEBUG_QE
	printf("[Aggregate::Aggregate] Relation attirbute prefix: %s\n", this->relation.c_str());
#endif
	int pos = aggAttr.name.find('.');
	this->aggAttr.name = aggAttr.name.substr(pos + 1, aggAttr.name.length() - pos + 1);

	getAggregateResults();
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op)
{
	this->input = input;
	this->hasGroupBy = true;
	input->getAttributes(this->attrs);
	this->op = op;
	this->aggAttr = aggAttr;
	this->total = 1;

	for (int i = 0; i < this->attrs.size(); ++i)
	{
		int pos = attrs[i].name.find('.');
		string attributeName = attrs[i].name.substr(pos + 1, attrs[i].name.length() - pos + 1);
		this->relation = attrs[i].name.substr(0, pos);
		this->attrs[i].name = attributeName;
	}
#ifdef DEBUG_QE
	printf("[Aggregate::Aggregate] Relation attirbute prefix: %s\n", this->relation.c_str());
#endif
	int pos = aggAttr.name.find('.');
	this->aggAttr.name = aggAttr.name.substr(pos + 1, aggAttr.name.length() - pos + 1);

	this->groupAttr = groupAttr;

	pos = groupAttr.name.find('.');
	this->groupAttr.name = groupAttr.name.substr(pos + 1, groupAttr.name.length() - pos + 1);

	getAggregateResults();
}

void Aggregate::getAggregateResults()
{
	void *data = malloc(PAGE_SIZE);
	while(input->getNextTuple(data) != RM_EOF)
	{
#ifdef DEBUG_QE
		RelationManager::instance()->printTuple(this->attrs, data);
#endif
		void *value = malloc(PAGE_SIZE);
		int rc = getValueOfAttrByName(data, this->attrs, this->aggAttr.name, value);
		if (rc == -1)
		{
#ifdef DEBUG_QE
			printf("[Aggregate::getAggregateResults] Null value encountered! What to do with Null?\n");
#endif
		}

		float v;

		switch(this->aggAttr.type)
		{
			case TypeInt:
				int temp;
				memcpy(&temp, value, sizeof(int));
				v = (float) temp;
				break;
			case TypeReal:
				memcpy(&v, value, sizeof(TypeReal));
				break;
		}

		if (this->hasGroupBy)
		{
			void *key_value = malloc(PAGE_SIZE);
			rc = getValueOfAttrByName(data, this->attrs, this->groupAttr.name, key_value);

			switch(this->groupAttr.type)
			{
				case TypeInt:
				{
					int intKey;
					memcpy(&intKey, key_value, sizeof(int));

					unordered_map<int, GroupAttr>::iterator gotInt = this->intMap.find (intKey);
					if (gotInt == this->intMap.end())
					{
						GroupAttr temp;
						temp.sum += v;
						temp.count++;
						intMap[intKey] = temp;
					}
					else
					{
						gotInt->second.sum += v;
						gotInt->second.count++;
						gotInt->second.max = v > (gotInt->second.max) ? v : gotInt->second.max;
						gotInt->second.min = v < (gotInt->second.min) ? v : gotInt->second.min;
					}
					break;
				}
				case TypeReal:
				{
					float floatKey;
					memcpy(&floatKey, key_value, sizeof(float));

					unordered_map<float, GroupAttr>::iterator gotFloat = this->floatMap.find (floatKey);
					if (gotFloat == this->floatMap.end())
					{
						GroupAttr temp;
						floatMap[floatKey] = temp;
						temp.sum += v;
						temp.count++;
					}
					else
					{
						gotFloat->second.sum += v;
						gotFloat->second.count++;
						gotFloat->second.max = v > (gotFloat->second.max) ? v : gotFloat->second.max;
						gotFloat->second.min = v < (gotFloat->second.min) ? v : gotFloat->second.min;
					}
					break;
				}
				case TypeVarChar:
				{
					int nameLength;
					char* key_c = (char *) malloc(nameLength + 1);
					memcpy(&nameLength, key_value, sizeof(int));
					memcpy(key_c, key_value, nameLength);

					key_c[nameLength] = '\0';
					string stringKey = string(key_c);

					unordered_map<string, GroupAttr>::iterator gotString = this->stringMap.find (stringKey);
					if (gotString == this->stringMap.end())
					{
						GroupAttr temp;
						stringMap[stringKey] = temp;
						temp.sum += v;
						temp.count++;
					}
					else
					{
						gotString->second.sum += v;
						gotString->second.count++;
						gotString->second.max = v > (gotString->second.max) ? v : gotString->second.max;
						gotString->second.min = v < (gotString->second.min) ? v : gotString->second.min;
					}
					break;
				}
			}

			switch(this->groupAttr.type)
			{
				case TypeInt:
					this->intIterator = this->intMap.begin();
					this->total = intMap.size();
					break;
				case TypeReal:
					this->floatIterator = this->floatMap.begin();
					this->total = floatMap.size();
					break;
				case TypeVarChar:
					this->stringIterator = this->stringMap.begin();
					this->total = stringMap.size();
					break;
			}
			free(key_value);
		} 
		else
		{
			this->count++;
			this->sum += v;
			if (v > this->max)
				this->max = v;
			if (v < this->min)
				this->min = v;
		}
		free(value);
	}

	free(data);
}

RC Aggregate::getNextTuple(void *data)
{
	printf("total number of keys: %d\n", this->total);
	if (current >= total)
		return QE_EOF;

	int nFields = 1;
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    nullFieldsIndicator[0] = 0;
    int offset = nullFieldsIndicatorActualSize;
    float res;

    if (hasGroupBy)
    {
    	GroupAttr groupRes;
    	switch(this->groupAttr.type)
		{
			case TypeInt:
				memcpy((char *)data + offset, &(intIterator->first), sizeof(int));
				offset += sizeof(int);
				groupRes = intIterator->second;
				intIterator++;
				break;
			case TypeReal:
				memcpy((char *)data + offset, &(floatIterator->first), sizeof(float));
				offset += sizeof(float);
				groupRes = floatIterator->second;
				floatIterator++;
				break;
			case TypeVarChar:
				int nameLength = stringIterator->first.length();
				memcpy((char *)data + offset, &nameLength, sizeof(int));
				offset += sizeof(int);
				memcpy((char *)data + offset, &(stringIterator->first), sizeof(int));
				offset += sizeof(nameLength);
				groupRes = stringIterator->second;
				stringIterator++;
				break;
		}

		switch (this->op)
		{
			case MIN:
				res = groupRes.min;
				break;
			case MAX:
				res = groupRes.max;
				break;
			case COUNT:
				res = groupRes.count;
				break;
			case AVG:
				res = groupRes.sum / groupRes.count;
				break;
			case SUM:
				res = groupRes.sum;
				break;
    	} 
    }
    else 
    {
		switch (this->op)
		{
			case MIN:
				res = this->min;
				break;
			case MAX:
				res = this->max;
				break;
			case COUNT:
				res = this->count;
				break;
			case AVG:
				res = this->sum / this->count;
				break;
			case SUM:
				res = this->sum;
				break;
		}
	}

	memcpy((char*) data + offset, &res, sizeof(float));
	offset += sizeof(float);
    current++;

    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    free(nullFieldsIndicator);
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();

	if (hasGroupBy)
	{
	    string tmp = this->relation + "." + this->groupAttr.name;
	    Attribute attr = this->groupAttr;
	    attr.name = tmp;
		attrs.push_back(attr);
	}

	//Put aggregate op name;
	string tmp = getOpName();
    tmp += "(";
    tmp += this->relation + "." + this->aggAttr.name;
    tmp += ")";
    Attribute attr = this->aggAttr;
    attr.name = tmp;
	attrs.push_back(attr);
}

string Aggregate::getOpName() const
{
	switch (this->op)
	{
		case MIN:
			return "MIN";
			break;
		case MAX:
			return "MAX";
			break;
		case COUNT:
			return "COUNT";
			break;
		case AVG:
			return "AVG";
			break;
		case SUM:
			return "SUM";
			break;
	}
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages)
{
	this->leftInput = leftIn;
	this->rightInput = rightIn;
	this->condition = &condition;
	this->numOfPages = numPages;
	this->fail = false;
	// leftIn->getAttributes(this->leftAttrs);

	// for (int i = 0; i < this->leftAttrs.size(); ++i)
	// {
	// 	int pos = this->leftAttrs[i].name.find('.');
	// 	string attributeName = this->leftAttrs[i].name.substr(pos + 1, this->leftAttrs[i].name.length() - pos + 1);
	// 	this->leftTable = this->leftAttrs[i].name.substr(0, pos);
	// 	this->leftAttrs[i].name = attributeName;
	// }

	// rightIn->getAttributes(this->rightAttrs);

	// for (int i = 0; i < this->rightAttributes.size(); ++i)
	// {
	// 	int pos = this->rightAttributes[i].name.find('.');
	// 	string attributeName = this->rightAttributes[i].name.substr(pos + 1, this->rightAttributes[i].name.length() - pos + 1);
	// 	this->rightTable = this->rightAttributes[i].name.substr(0, pos);
	// 	this->rightAttributes[i].name = attributeName;
	// }

	leftIn->getAttributes(leftAttrs);

	rightIn->getAttributes(rightAttrs);

	rightTuple = malloc(PAGE_SIZE);

	this->firstTuple = true;
}

RC BNLJoin::loadBlock()
{
	freeBlock();
	int sizeCount = 0;
	// cout<<"Load block"<<endl;
	while (sizeCount < numOfPages * PAGE_SIZE) 
	{
		void *data = malloc(PAGE_SIZE);
		memset(data, 0, PAGE_SIZE);
		if(leftInput->getNextTuple(data) == QE_EOF)
		{
			free(data);
			if(sizeCount == 0) return QE_EOF;
			break;
		}

		vector<Attribute> attrs;
		attrs = this->leftAttrs;
		for(int i =0; i <attrs.size(); i++)
			attrs[i].name = getOriginalAttrName(attrs[i].name);
		
		// RelationManager::instance()->printTuple(attrs, data);
		int size = RelationManager::instance()->getSizeOfdata(leftAttrs, data);
		sizeCount += size;
		block.push_back(data);
	}
	count = 0;
	return 0;
}

RC BNLJoin::freeBlock()
{
	while(block.size() != 0)
	{
		free(block.front());
		block.erase(block.begin());
	}
	return 0;
}

RC BNLJoin::getNextTuple(void *data)
{
	if(firstTuple)
	{
		if((loadBlock() == QE_EOF) || (rightInput->getNextTuple(rightTuple) == QE_EOF))
		{
			return QE_EOF;
		}
		firstTuple = false;
	}

	if(block.size() == 0)
	{
		return QE_EOF;
	}

	do
	{
		if(count >= block.size()) //no more tuples in the left block, has two cases: still has tuples in the right; no more tuples in the right
		{
			
			memset(rightTuple, 0, PAGE_SIZE);
			if(rightInput->getNextTuple(rightTuple) == QE_EOF)	//Right: get next tuple
			{								//no more tuples in the right, need to load next block in the left and scan in the right again
				if(loadBlock() == QE_EOF)  //Left: load next block 
				{
					return QE_EOF;	//no more blocks, no more tuples, done
				}
				else  //Right: scan from the first tuple again
				{
					rightInput->setIterator();
					rightInput->getNextTuple(rightTuple);
				}
			}
#ifdef DEBUG_QE
			vector<Attribute> attrs;
			attrs = this->rightAttrs;
			for(int i =0; i <attrs.size(); i++)
			attrs[i].name = getOriginalAttrName(attrs[i].name);
			cout<<"[BNL join] right Tuple--------------: ";
			RelationManager::instance()->printTuple(attrs, rightTuple);
#endif
			count = 0;
		}
		leftTuple = block[count];
#ifdef DEBUG_QE
		vector<Attribute> attrs2;
		attrs2 = this->leftAttrs;
		for(int i =0; i <attrs2.size(); i++)
		attrs2[i].name = getOriginalAttrName(attrs2[i].name);
		cout<<"[BNL join]******************left Tuple: ";
		RelationManager::instance()->printTuple(attrs2, leftTuple);
#endif

		count++;
		//cout<<"cout:"<<count<<endl;
	} while(!isConditionEqual());

	if(this->fail)
	{
		this->fail = false;
		return -1;
	}

	concatenateLeftAndRight(leftTuple, rightTuple, data, this->leftAttrs, this->rightAttrs);
	
	return 0;
}

bool BNLJoin::isConditionEqual()
{
	void *value1 = malloc(PAGE_SIZE);

	vector<Attribute> left_attrs;
	left_attrs = this->leftAttrs;
	for(int i =0; i <leftAttrs.size(); i++)
	left_attrs[i].name = getOriginalAttrName(leftAttrs[i].name);

	vector<Attribute> right_attrs;
	right_attrs = this->rightAttrs;
	for(int i =0; i <right_attrs.size(); i++)
	right_attrs[i].name = getOriginalAttrName(right_attrs[i].name);

	int index1 = getValueOfAttrByName(leftTuple, left_attrs, getOriginalAttrName(condition->lhsAttr), value1);
	void *value2 = malloc(PAGE_SIZE);
	int index2 = getValueOfAttrByName(rightTuple, right_attrs, getOriginalAttrName(condition->rhsAttr), value2);

	if(index1 < 0 || index2 < 0) 
	{
		this->fail = true;
		return true;
	}
	else this->fail = false;
		
	// cout<<"value1: type:"<<leftAttrs[index1].type<<" value:"<<v1<<"  value2: type:"<<rightAttrs[index2].type<<" value:"<<v2<<endl;
	bool satisfied = false;
	//cout<<"[BNL Join]"<<" index1:"<<index1<<" index2:"<<index2<<endl;
	if(left_attrs[index1].type == right_attrs[index2].type && left_attrs[index1].type == right_attrs[index2].type)
		satisfied = isEqual(value1, value2, &leftAttrs[index1]);
	free(value1);
	free(value2);
	return satisfied;
}

void BNLJoin::getAttributes(vector<Attribute>& attrs) const
{
	attrs.clear();
	attrs = this->leftAttrs;
	attrs.insert(attrs.end(), this->rightAttrs.begin(), this->rightAttrs.end());
}

BNLJoin::~BNLJoin()
{
	freeBlock();
	free(rightTuple);
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
{
	this->condition = &condition;
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	leftIn->getAttributes(this->leftAttributes);

	for (int i = 0; i < this->leftAttributes.size(); ++i)
	{
		int pos = this->leftAttributes[i].name.find('.');
		string attributeName = this->leftAttributes[i].name.substr(pos + 1, this->leftAttributes[i].name.length() - pos + 1);
		this->leftTable = this->leftAttributes[i].name.substr(0, pos);
		this->leftAttributes[i].name = attributeName;
	}

	rightIn->getAttributes(this->rightAttributes);

	for (int i = 0; i < this->rightAttributes.size(); ++i)
	{
		int pos = this->rightAttributes[i].name.find('.');
		string attributeName = this->rightAttributes[i].name.substr(pos + 1, this->rightAttributes[i].name.length() - pos + 1);
		this->rightTable = this->rightAttributes[i].name.substr(0, pos);
		this->rightAttributes[i].name = attributeName;
	}
}
INLJoin::~INLJoin()
{
	if (this->leftData != NULL)
	{
		free(leftValue);
		free(leftData);
	}
}

RC INLJoin::getNextTuple(void *data)
{
	void *rightData = malloc(PAGE_SIZE);
	while(1)
	{
		if (this->leftData == NULL)
		{
#ifdef DEBUG_QE
			printf("[INLJoin::getNextTuple] Loading tuple from left input\n");
#endif
			this->leftData = malloc(PAGE_SIZE);
			this->leftValue = malloc(PAGE_SIZE);
			if (this->leftIn->getNextTuple(this->leftData) == QE_EOF)
			{
				free(rightData);
				return QE_EOF;
			}
			int pos = getValueOfAttrByName(this->leftData, this->leftAttributes, getOriginalAttrName(this->condition->lhsAttr), 
					this->leftValue);
			this->rightIn->setIterator(this->leftValue, this->leftValue, true, true);
#ifdef DEBUG_QE
			RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
#endif
		}

		if (this->rightIn->getNextTuple(rightData) != QE_EOF)
		{
#ifdef DEBUG_QE
			printf("[INLJoin::getNextTuple] Loading tuple from right input\n");
			RelationManager::instance()->printTuple(this->rightAttributes, rightData);
#endif
			concatenateLeftAndRight(this->leftData, rightData, data, this->leftAttributes, this->rightAttributes);
			free(rightData);
			return 0;
		} 
		else
		{
			if (this->leftIn->getNextTuple(this->leftData) != QE_EOF)
			{
#ifdef DEBUG_QE
				printf("[INLJoin::getNextTuple] Loading tuple from left input\n");
				RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
#endif
                int pos = getValueOfAttrByName(this->leftData, this->leftAttributes, getOriginalAttrName(this->condition->lhsAttr), 
                								this->leftValue);
                this->rightIn->setIterator(this->leftValue, this->leftValue, true, true);
				continue;
			} 
			else
			{
				free(rightData);
				return QE_EOF;
			}
		}
	}
	free(rightData);
}

bool INLJoin::canJoin(const void* leftData, const void* rightData)
{
	if (this->condition->bRhsIsAttr)
	{
		string leftAttrName = getOriginalAttrName(this->condition->lhsAttr);
		string rightAttrName = getOriginalAttrName(this->condition->rhsAttr);
		void *leftValue = malloc(PAGE_SIZE);
		void *rightValue = malloc(PAGE_SIZE);

		int leftPos = getValueOfAttrByName(leftData, this->leftAttributes, leftAttrName, leftValue);
		int rightPos = getValueOfAttrByName(rightData, this->rightAttributes, rightAttrName, rightValue);

		bool res;
		if (isEqual(leftValue, rightValue, &this->leftAttributes[leftPos]))
			res = true;
		else
			res = false;

		free(leftValue);
		free(rightValue);
		return res;
	}
	return false;
}

GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions)
{
	this->leftInput = leftIn;
	this->rightInput = rightIn;
	this->condition = &condition;
	this->numPartitions = numPartitions;
	this->fail = false;
	this->currentFileHandleIndex = 0;
	this->rightRbfm_scanIterator = 0;
	leftIn->getAttributes(leftAttributes);
	rightIn->getAttributes(rightAttributes);

	for (int i = 0; i < this->leftAttributes.size(); ++i)
	{
		int pos = this->leftAttributes[i].name.find('.');
		string attributeName = this->leftAttributes[i].name.substr(pos + 1, this->leftAttributes[i].name.length() - pos + 1);
		this->leftTable = this->leftAttributes[i].name.substr(0, pos);
		this->leftAttributes[i].name = attributeName;
		this->leftAttrNames.push_back(attributeName);
	}

	for (int i = 0; i < this->rightAttributes.size(); ++i)
	{
		int pos = this->rightAttributes[i].name.find('.');
		string attributeName = this->rightAttributes[i].name.substr(pos + 1, this->rightAttributes[i].name.length() - pos + 1);
		this->rightTable = this->rightAttributes[i].name.substr(0, pos);
		this->rightAttributes[i].name = attributeName;
		this->rightAttrNames.push_back(attributeName);
	}

	RC rc;
	rc = initRBFM();
	if(rc != 0)
	{
		cout<<"[GHJoin] initRBFM fail"<<endl;
		return;
	}
	rc = partitionFile();
	if(rc != 0){
		cout<<"[GHJoin] partitionFile fail"<<endl;
		return;
	}
	rc = loadLeftPartition();
	if(rc != 0){
		cout<<"[GHJoin] loadLeftPartition fail"<<endl;
		return;
	}
	loadRightPartition();
	if(rc != 0){
		cout<<"[GHJoin] loadRightPartition fail"<<endl;
		return;
	}
}

RC GHJoin::initRBFM()
{
	rbfm = RecordBasedFileManager::instance();
    for(int i = 1; i <= this->numPartitions; i++)
    {
        string leftFileName = getNameOfPartition(true, i);
        FileHandle *leftFileHandle = new FileHandle();
        rbfm->createFile(leftFileName);
		// cout<<"------partition left file name:"<<leftFileName<<endl;
		rbfm->openFile(leftFileName, *leftFileHandle);
        leftFileHandles.push_back(leftFileHandle);
        
        string rightFileName = getNameOfPartition(false, i);
        FileHandle *rightFileHandle = new FileHandle();
        rbfm->createFile(rightFileName);
		// cout<<"------partition right file name:"<<rightFileName<<endl;
		rbfm->openFile(rightFileName, *rightFileHandle);
        rightFileHandles.push_back(rightFileHandle);
    }
    return 0;
}

 RC GHJoin::partitionFile()
 {
	void *tuple = malloc(PAGE_SIZE);
	int hash;
    while(this->leftInput->getNextTuple(tuple) != QE_EOF)
    {
		hash = HashTuple(tuple, true);
        if(hash < 0)
        {
            this->fail = true;
			free(tuple);
			return -1;
        }
		RID rid;
		// cout<<"**********[GHJoin]******** all left records: ";
		// cout<<"hashValue:"<<hash<<"		";
		// rbfm->printRecord(leftAttributes,tuple);

		if(rbfm->insertRecord(*leftFileHandles[hash], leftAttributes, tuple, rid) != 0)
    	{
			cout<<"[GHJoin] insert left fail !!!!!!!!!!!!!!!!"<<endl;
			this->fail = true;
			free(tuple);
        	return -1;
    	}
		// cout<<"[][][]][]"<<hash<<" "<<rid.pageNum<<" "<<rid.slotNum<<endl;

		// cout<<"+++++++++++++hash:"<<hash<<endl;
		// void *d = malloc(PAGE_SIZE);
		// rbfm->readRecord(*leftFileHandles[hash],leftAttributes,r,d);
		// rbfm->printRecord(leftAttributes,d);
		// free(d);
		memset(tuple, 0 ,PAGE_SIZE);
    }
    while(this->rightInput->getNextTuple(tuple) != QE_EOF)
    {
		hash = HashTuple(tuple, false);
        if(hash < 0)
        {
            this->fail = true;
			free(tuple);
			return -1;
        }
		RID rid;
		// cout<<"**********[GHJoin]******** all right records: ";
		// cout<<"hashValue:"<<hash<<"		";
		// rbfm->printRecord(rightAttributes,tuple);

		if(rbfm->insertRecord(*rightFileHandles[hash], rightAttributes, tuple, rid) != 0)
    	{
			cout<<"[GHJoin] insert right fail !!!!!!!!!!!!!!!!"<<endl;
			this->fail = true;
			free(tuple);
        	return -1;
    	}
        memset(tuple, 0 ,PAGE_SIZE);
    }
    free(tuple);
	return 0;
}

// RC GHJoin::freeHashMap()
// {
// 	// for(int i = 0;i<HashMap.size();i++)
// 	// {

// 	// }
// 	HashMap.clear();
// 	return 0;
// }

RC GHJoin::freeinPartitionHashMap()
{
	
	for ( auto it = intMap.begin(); it != intMap.end(); ++it )
	{
		free(it->second);
	}
	for ( auto it = floatMap.begin(); it != floatMap.end(); ++it )
	{
		free(it->second);
	}
		for ( auto it = stringMap.begin(); it != stringMap.end(); ++it )
	{
		free(it->second);
	}
	// for(int i = 0; i <= intMap.size(); i++)
	// {
	// 	free(intMap[i].second);
	// }
	// for(int i = 0; i <= intMap.size(); i++)
	// {
	// 	free(floatMap[i].second);
	// }
	// for(int i = 0; i <= intMap.size(); i++)
	// {
	// 	free(stringMap[i].second);
	// }
	intMap.clear();
	floatMap.clear();
	stringMap.clear();
	return 0;
}

RC GHJoin::loadLeftPartition()
{
	freeinPartitionHashMap();
	// for(int i=0;i<numPartitions;i++)
	// {
	// 	vector<void *> v1;
	// 	vector<vector<void *>> v2;
	// 	v2.push_back(v1);
	// 	inPartitionHashMap.push_back(v2);
	// }
	
	bool hasData = false;
	RBFM_ScanIterator leftRbfm_scanIterator;
	rbfm->scan(*(leftFileHandles[currentFileHandleIndex]), leftAttributes, "", NO_OP, NULL, leftAttrNames, leftRbfm_scanIterator);

	
		// void *d = malloc(PAGE_SIZE);
		// rbfm->readRecord(*(leftFileHandles[currentFileHandleIndex]),leftAttributes, r ,d);
		// rbfm->printRecord(leftAttributes,d);
		// cout<<"curr filehandle index"<<currentFileHandleIndex<<endl;

	RID rid;
	void *record = malloc(PAGE_SIZE);
	while(leftRbfm_scanIterator.getNextRecord(rid, record) != RBFM_EOF)
	{
		hasData = true;;
		void *value = malloc(PAGE_SIZE);
		int length = rbfm->getRecordLength(leftAttributes, record);
		void *tuple = malloc(length);
		memcpy((char *)tuple, (char *)record, length);
		int index = getValueOfAttrByName(record, leftAttributes, getOriginalAttrName(condition->lhsAttr), value);
		
		// int inPartitionHashValue = inPartitionHashFunction(value, rightAttributes[index].type);
		// vector<void *> v;
		// v.push_back(value);
		// v.push_back(record);
		// inPartitionHashMap[inPartitionHashValue].push_back(v);

		// cout<<"[GHJoin] [Load left partition] ";
		// rbfm->printRecord(leftAttributes, record);

		switch(leftAttributes[index].type)
		{
			case TypeInt:{
				int n;
				memcpy(&n, (char *)value, sizeof(int));
				intMap.insert(make_pair(n,tuple));
				break;
			}
			case TypeReal:{
				float f;
				memcpy(&f, (char *)value, sizeof(float));
				floatMap.insert(make_pair(f,tuple));
				break;
			}
			case TypeVarChar:{
				string str;
				int offset = 0;
				int strLength;
				memcpy(&strLength, (char *)value + offset, sizeof(int));
				offset += sizeof(int);
				char *str_c = (char*) malloc(strLength + 1);
				memcpy(str_c, (char *)value + offset, strLength);
				str_c[strLength] = '\0';
				str = string(str_c);
				stringMap.insert(make_pair(str,tuple));
				free(str_c);      
				break;
			}
		}
		free(value);
	}
	leftRbfm_scanIterator.close();
	free(record);
	if(!hasData && currentFileHandleIndex + 1 >= (int)numPartitions)
	{
		return -1;
	}

	return 0;
}

RC GHJoin::loadRightPartition()
{
	if(this->rightRbfm_scanIterator != 0)
    {
        rightRbfm_scanIterator->close();
        delete rightRbfm_scanIterator;
        rightRbfm_scanIterator = 0;
	}
	this->rightRbfm_scanIterator = new RBFM_ScanIterator();
	rbfm->scan(*(rightFileHandles[currentFileHandleIndex]), rightAttributes, "", NO_OP, NULL, rightAttrNames, *rightRbfm_scanIterator);
	return 0;
}

int GHJoin::inPartitionHashMapFind(void * tuple1, Attribute attr, int index)
{
	for(int i=0;i<inPartitionHashMap[index].size();i++)
	{
		int type = getAttributeType(inPartitionHashMap[index].at(i).at(2));
		if(compareAttributeType(type,attr.type))
		{
			if(inPartitionHashMapEquals(tuple1, inPartitionHashMap[index].at(i).at(0), attr))
			{
				return i;
			}
		}
	}
	return -1;
}

RC GHJoin::setattribute(int type, void *data)
{
	memcpy((char *)data, &type, sizeof(float));
	return 0;
}

int GHJoin::getAttributeType(void *data)
{
	int type;
	memcpy(&type, (char *)data, sizeof(float));
	return type;
}

bool GHJoin::compareAttributeType(int type1, AttrType type2)
{
	return type1 == (int)type2;
}

RC GHJoin::getNextTuple(void *data)
{
	bool findData = false;
	do{
		findData = false;
		if(this->currentFileHandleIndex >= (int)this->numPartitions)
				return QE_EOF;
		RID rid;
		void *tuple = malloc(PAGE_SIZE);
		bool reachEOF = true;
		while(rightRbfm_scanIterator->getNextRecord(rid, tuple) != RBFM_EOF)
		{
			reachEOF = false;
			void *value = malloc(PAGE_SIZE);
			int index = getValueOfAttrByName(tuple, rightAttributes, getOriginalAttrName(condition->rhsAttr), value);
			// int inPartitionHashValue = inPartitionHashFunction(value, rightAttributes[index].type);
			// int inPartitionHashMapIndex = inPartitionHashMapFind(tuple, rightAttributes[index], inPartitionHashValue);
//#ifdef DEBUG_QE
			// cout<<"[GHJoin] scan right tuple:";
			// rbfm->printRecord(rightAttributes,tuple);
//#endif


			switch(rightAttributes[index].type)
			{
				case TypeInt:{
					int n;
					memcpy(&n, (char *)value, sizeof(int));
					auto ite = intMap.find(n);
					if (ite != intMap.end()) {
						concatenateLeftAndRight(ite->second, tuple, data, leftAttributes, rightAttributes);
						findData = true;
					}
					break;
				}
				case TypeReal:{
					float f;
					memcpy(&f, (char *)value, sizeof(float));
					auto ite = floatMap.find(f);
					if (ite != floatMap.end()) {
		//#ifdef DEBUG_QE
						// cout<<"[GHJoin] left tuple:  ";
						// rbfm->printRecord(leftAttributes,ite->second);
		//#endif
						concatenateLeftAndRight(ite->second, tuple, data, leftAttributes, rightAttributes);
		#ifdef DEBUG_QE
						vector<Attribute> attr;
						getAttributes(attr);
						cout<<"[GHJoin] join tuple:  ";
						rbfm->printRecord(attr,data);
		#endif		
						findData = true;
					}
					break;
				}
				case TypeVarChar:{
					string str;
					int offset = 0;
					int strLength;
					memcpy(&strLength, (char *)value + offset, sizeof(int));
					offset += sizeof(int);
					char *str_c = (char*) malloc(strLength + 1);
					memcpy(str_c, (char *)value + offset, strLength);
					str_c[strLength] = '\0';
					str = string(str_c);
					auto ite = stringMap.find(str);
					if (ite != stringMap.end()) {
						concatenateLeftAndRight(ite->second, tuple, data, leftAttributes, rightAttributes);
						findData = true;
					}

					// cout<<"[GHJoin] left tuple:  ";
					// rbfm->printRecord(leftAttributes,ite->second);

					// vector<Attribute> attr;
					// 	getAttributes(attr);
					// 	cout<<"[GHJoin] join tuple:  ";
					// 	rbfm->printRecord(attr, data);

					free(str_c);      
					break;
				}
			}
			free(value);

			if(findData)
			{
				break;
			}
		}
		free(tuple);
		if(reachEOF) //reach EOF, load next partition
		{
			this->currentFileHandleIndex++;
			if(this->currentFileHandleIndex >= (int)this->numPartitions)
				return QE_EOF;
			
			//cout<<"[GHJoin] load next partition"<<endl;

			loadLeftPartition();
			loadRightPartition();
		}
	}while(!findData);

	return 0;
}



void GHJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = this->leftAttributes;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = this->leftTable;
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }

    Attribute t;
    for(i = 0; i < this->rightAttributes.size(); ++i)
    {
        string tmp = this->rightTable;
        tmp += ".";
        tmp += this->rightAttributes.at(i).name;

        t = this->rightAttributes[i];
        t.name = tmp;
        attrs.push_back(t);
    }
}

GHJoin::~GHJoin()
{
	for(int i=0;i<numPartitions;i++)
    {
        rbfm->closeFile(*(leftFileHandles[i]));
		rbfm->destroyFile(getNameOfPartition(true, i+1));
		rbfm->closeFile(*(rightFileHandles[i]));
		rbfm->destroyFile(getNameOfPartition(false, i+1));
    }
	freeinPartitionHashMap();

	for(unsigned i = 0; i < numPartitions; i++)
    {
        delete leftFileHandles.at(i);
        delete rightFileHandles.at(i);
    }
	leftFileHandles.clear();
	rightFileHandles.clear();
	if(rightRbfm_scanIterator != 0)
    {
        delete rightRbfm_scanIterator;
        rightRbfm_scanIterator = 0;
    }
	leftAttributes.clear();
	rightAttributes.clear();
	leftAttrNames.clear();
	rightAttrNames.clear();
}

int GHJoin::HashTuple(void *tuple, bool isLeft)
 {
	unsigned hashValue;
	if(isLeft)
	{
		hashValue = getHashValue(tuple, leftAttributes,condition->lhsAttr);
	}
	else
	{
		hashValue = getHashValue(tuple, rightAttributes,condition->rhsAttr);
	}

	if(this->fail) return -1;
    
 	return hashValue;
 }

 bool GHJoin::inPartitionHashMapEquals(void *value1,void *value2, Attribute &attr)
 {
	return isEqual(value1, value2, &attr);
 }

 unsigned GHJoin::inPartitionHashFunction(void *key, AttrType type)
{
	unsigned hashValue;
	switch(type)
	{
	case TypeInt:
		hashValue = (hashTypeInt(key) % this->numPartitions);
		hashValue = hashValue * 17 + 23;
		hashValue = hashValue % this->numPartitions;
		break;
	case TypeReal:
		hashValue = hashTypeReal(key) % this->numPartitions;
		hashValue = hashValue * 11 + 19;
		hashValue = hashValue % this->numPartitions;
		break;
	case TypeVarChar:
		hashValue = hashTypeVarChar(key) % this->numPartitions;
		hashValue = hashValue * 5 + 31;
		hashValue = hashValue % this->numPartitions;
		break;
	}
	return hashValue;
}

unsigned GHJoin::getHashValue(void *tuple, vector<Attribute> Attributes, string hsAttr)
{
	void *value = malloc(PAGE_SIZE);
	int index;
	unsigned hashValue;
	index = getValueOfAttrByName(tuple, Attributes, getOriginalAttrName(hsAttr), value);
	if(index < 0)
	{
		this->fail = true;
	}
	// float v;
	// memcpy(&v, (char *)value, sizeof(float));
	// cout<<"[GHJoin] data:"<<v<<" type:"<<Attributes[index].type<<endl;
	switch(Attributes[index].type)
	{
	case TypeInt:
		hashValue = hashTypeInt(value) % this->numPartitions;
		break;
	case TypeReal:
		hashValue = hashTypeReal(value) % this->numPartitions;
		break;
	case TypeVarChar:
		hashValue = hashTypeVarChar(value) % this->numPartitions;
		break;
	}
	
	free(value);
	return hashValue;
}

unsigned int hashTypeInt(void* data)
{
	int n;
	memcpy(&n, (char *)data, sizeof(int));
	n += ~(n << 15);
    n ^=  (n >> 10);
    n +=  (n << 3);
    n ^=  (n >> 6);
    n += ~(n << 11);
    n ^=  (n >> 16);
    return n;
}

 unsigned int hashTypeReal(void *data)
{
	float f;
	memcpy(&f, (char *)data, sizeof(float));
	float f2 = (int)f*7+13;
    void *n = malloc(sizeof(float));
	memcpy(n, &f2, sizeof(float));
    return hashTypeInt(n);
}

unsigned hashTypeVarChar(void *data)
{
	int nameLength;
	int offset = 0;
	// string str;
	memcpy(&nameLength, (char *)data + offset, sizeof(int));
	offset += sizeof(int);
	char *str_c = (char *) malloc(nameLength + 1);
	memcpy(str_c, (char *)data + offset, nameLength);
	str_c[nameLength] = '\0';
	// str = string(str_c);
	
    unsigned long hash = 17;
    int c;
	int count = 0;
    while (c = *str_c++) {
        hash = ((hash << 2) + hash) + c; 
		count++;
		if(count >= nameLength - 1) break;
    }
	// cout<<"String:"<<str<<"  hashValue:"<<hash<<endl;
    return hash;
}

string GHJoin::getNameOfPartition(bool isLeft, int index)
{
	std::stringstream res;
	if(isLeft)
		res << "left_";
	else res << "right_";
    res << "join";
    res << index;
    res << "_";
	if(isLeft)
		res << (this->condition->lhsAttr);
	else res << (this->condition->rhsAttr);
    
    return res.str();
}

void concatenateLeftAndRight(const void* leftData, const void* rightData, void* data, vector<Attribute> &leftAttributes, vector<Attribute> &rightAttributes)
{
#ifdef DEBUG_QE
    printf("[concatenateLeftAndRight] concatenate between:\n");
    RelationManager::instance()->printTuple(this->leftAttributes, this->leftData);
    RelationManager::instance()->printTuple(this->rightAttributes, rightData);
#endif
	int nLeftFields = leftAttributes.size();
    int nullLeftFieldsIndicatorActualSize = ceil((double) nLeftFields / CHAR_BIT);
    unsigned char *nullLeftFieldsIndicator = (unsigned char *)malloc(nullLeftFieldsIndicatorActualSize);
    memcpy(nullLeftFieldsIndicator, leftData, nullLeftFieldsIndicatorActualSize);

	int nRightFields = rightAttributes.size();
    int nullRightFieldsIndicatorActualSize = ceil((double) nRightFields / CHAR_BIT);
    unsigned char *nullRightFieldsIndicator = (unsigned char *)malloc(nullRightFieldsIndicatorActualSize);
    memcpy(nullRightFieldsIndicator, rightData, nullRightFieldsIndicatorActualSize);

    int nFields = leftAttributes.size() + rightAttributes.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);

    int newOffset = nullFieldsIndicatorActualSize;
    int leftOffset = nullLeftFieldsIndicatorActualSize;
    int rightOffset = nullRightFieldsIndicatorActualSize;

    for (int i = 0; i < nFields; ++i)
    {
    	if (i < nLeftFields)
    	{
    		int nByte = i / 8;
	        int nBit = i % 8;
	        bool nullBit = nullLeftFieldsIndicator[nByte] & (1 << (7 - nBit));
#ifdef DEBUG_QE
		printf("[concatenateLeftAndRight] %d field is null ? %d\n", i, nullBit);
#endif
	        if (nullBit)
	        	nullFieldsIndicator[nByte] |= (1 << (7 - nBit));
	        else
	        {
	        	copyAttribute(leftData, leftOffset, data, newOffset, leftAttributes[i]);
	        }
    	}
    	else
    	{
    		int nByte = i / 8;
    		int nBit = i % 8;
    		int nRightByte = (i - nLeftFields) / 8;
    		int nRightBit = (i - nLeftFields) % 8;
    		bool nullBit = nullRightFieldsIndicator[nRightByte] & (1 << (7 - nRightBit));
#ifdef DEBUG_QE
                printf("[concatenateLeftAndRight] %d field is null ? %d\n", i, nullBit);
#endif

    		if (nullBit)
	        	nullFieldsIndicator[nByte] |= (1 << (7 - nBit));
	        else
	        {
	        	copyAttribute(rightData, rightOffset, data, newOffset, rightAttributes[i - nLeftFields]);
	        }
    	}
    }

    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
#ifdef DEBUG_QE
    printf("[concatenateLeftAndRight] After concatenation\n");
    RelationManager::instance()->printTuple(leftAttributes, data);
#endif
    free(nullLeftFieldsIndicator);
    free(nullRightFieldsIndicator);
    free(nullFieldsIndicator);
}

void copyAttribute(const void *from, int &fromOffset, void* to, int &toOffset, const Attribute &attr)
{
#ifdef DEBUG_QE
	printf("[copyAttribute] Copying attribute %s, fromOffset: %d, toOffset: %d\n", attr.name.c_str(), fromOffset, toOffset);
#endif
	switch(attr.type)
	{
		case TypeInt:
#ifdef DEBUG_QE
			int t;
			memcpy(&t, (char *)from + fromOffset, attr.length);
			printf("[copyAttribute] Value of attribute is %d\n", t);
#endif
			memcpy((char *)to + toOffset, (char *)from + fromOffset, attr.length);
			fromOffset += attr.length;
			toOffset += attr.length;
			break;
		case TypeReal:
#ifdef DEBUG_QE
                        float f;
                        memcpy(&f, (char *)from + fromOffset, attr.length);
                        printf("[copyAttribute] Value of attribute is %f\n", f);
#endif
			memcpy((char *)to + toOffset, (char *)from + fromOffset, attr.length);
			fromOffset += attr.length;
			toOffset += attr.length;
			break;
		case TypeVarChar:
			int nameLength;
			memcpy(&nameLength, (char *)from + fromOffset, sizeof(int));
			memcpy((char *)to + toOffset, &nameLength, sizeof(int));
			fromOffset += sizeof(int);
			toOffset += sizeof(int);
			memcpy((char *)to + toOffset, (char *)from + fromOffset, nameLength);
			fromOffset += nameLength;
			toOffset += nameLength;
			break;
	}
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = this->leftAttributes;
    unsigned i;

    // For attribute in vector<Attribute>, name it as rel.attr
    for(i = 0; i < attrs.size(); ++i)
    {
        string tmp = this->leftTable;
        tmp += ".";
        tmp += attrs.at(i).name;
        attrs.at(i).name = tmp;
    }

    Attribute t;
    for(i = 0; i < this->rightAttributes.size(); ++i)
    {
        string tmp = this->rightTable;
        tmp += ".";
        tmp += this->rightAttributes.at(i).name;

        t = this->rightAttributes[i];
        t.name = tmp;
        attrs.push_back(t);
    }
}

int getValueOfAttrByName(const void *data, vector<Attribute> &attrs, string attributeName, void* value)
{
	int pos;
	for (pos = 0; pos < attrs.size(); ++pos)
	{
		// printf("attributename: %s, attrs[pos].name: %s\n", attributeName.c_str(), attrs[pos].name.c_str());
		if (attrs[pos].name == attributeName)
			break;
	}

	if (pos == attrs.size())
		//attribute not found
		return -2;

#ifdef DEBUG_QE
		printf("[getValueOfAttrByName] position of the attribute %s: %d\n", attributeName.c_str(), pos);
#endif

	int nFields = attrs.size();
    int nullFieldsIndicatorActualSize = ceil((double) nFields / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);

    for (int i = 0; i < nullFieldsIndicatorActualSize; ++i)
    {
        nullFieldsIndicator[i] = ((char *)data)[i];
    }
    int offset = nullFieldsIndicatorActualSize;

    int nByte = pos / 8;
    int nBit = pos % 8;
    bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
    int returnDataOffset = 1;
    int isNull = nullBit ? 1 : 0;

    if (isNull)
    	return -1;
    else
    {
    	for (int i = 0; i < nFields; ++i)
	    {
	        int nByte = i / 8;
	        int nBit = i % 8;
	        bool nullBit = nullFieldsIndicator[nByte] & (1 << (7 - nBit));
	        if(!nullBit)
	        {
	            if (attrs[i].type == TypeInt)
	            {
	            	//int v;
	                memcpy(value, (char *)data + offset, attrs[i].length);

	                //memcpy(&v, value, sizeof(int));
	                offset += attrs[i].length;
	                //printf("[getValueOfAttrByName]%s: %-10d\n", attrs[i].name.c_str(), v);
	            }
	            if (attrs[i].type == TypeReal)
	            {
	                memcpy(value, (char *)data + offset, attrs[i].length);
	                offset += attrs[i].length;
	                // printf("[getValueOfAttrByName]%s: %-10f\n", attrs[i].name.c_str(), value);
	            }
	            if (attrs[i].type == TypeVarChar)
	            {
	                int nameLength;
	                memcpy(&nameLength, (char *)data + offset, sizeof(int));
	                offset += sizeof(int);
	                // printf("String length: %d\n", nameLength);
	                memcpy(value, &nameLength, sizeof(int));
	                memcpy((char *)value + sizeof(int), (char *)data + offset, nameLength);
	                offset += nameLength;

	                //printf("%s: %-10s\t", attrs[i].name.c_str(), value);
	            }          
	         	if (i == pos)
		        {
		        	break;
		        }
	        }
	        else
	        {
	            // printf("%s: NULL\t", attrs[i].name.c_str());
	        }
	    }
    }

    //int v;
    // memcpy(&v, (char *)value, sizeof(int));
    // printf("[getValueOfAttrByName]return value %s: %-10d\n", attrs[pos].name.c_str(), value);
    free(nullFieldsIndicator);
    return pos;
}

bool compareCondition(const Attribute *attribute, const void* value, const Condition* condition)
{
	CompOp compOp = condition->op;
#ifdef DEBUG_QE
	int v;
	memcpy(&v, value, sizeof(int));
	printf("[QE::compareCondition] Value is %d\n", v);
#endif
	bool satisfied = false;
    switch (compOp)
    {
        case EQ_OP:
            satisfied = isEqual(value, condition->rhsValue.data, attribute);
            break;
        case LT_OP:
            satisfied = isLessThan(attribute, value, condition->rhsValue.data);
            break;
        case LE_OP:
            satisfied = isLessAndEqualThan(attribute, value, condition->rhsValue.data);
            // printf("is less and equal than: %d\n", satisfied);
            break;
        case GT_OP:
            satisfied = isLargerThan(attribute, value, condition->rhsValue.data);
            break;
        case GE_OP:
            satisfied = isLargerAndEqualThan(attribute, value, condition->rhsValue.data);
            break;
        case NE_OP:
            satisfied = !isEqual(value, condition->rhsValue.data, attribute);
            break;
        case NO_OP:
            satisfied = true;
            break;
    }
    return satisfied;
}

string getOriginalAttrName(const string s)
{
	int pos = s.find('.');
	return s.substr(pos + 1, s.length() - pos + 1);
}
// ... the rest of your implementations go here
