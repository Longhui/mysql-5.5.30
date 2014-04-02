/**
* 属性组解析器
*
* @author 李伟钊(liweizhao@corp.netease.com)
*/
#include "misc/ColumnGroupParser.h"
#include "misc/Parser.h"
#include "util/Array.h"
#include "util/SmartPtr.h"

using namespace ntse;

namespace ntse {

const char ColumnGroupParser::COL_COMMA_DELIMITER = ',';
const char * ColumnGroupParser::MSG_EXCEPTION_NON_EXISTED_COL_NAME = "Column name '%s' provided does NOT exist.";
const char * ColumnGroupParser::MSG_EXCEPTION_COLUMNGROUP_REDIFINED = "Column \"%s\" is redefined in some column groups.";
const char * ColumnGroupParser::MSG_EXCEPTION_COLUMNGROUP_NOT_DEFINED = "Column \"%s\" is not defined in any column group.";
const char * ColumnGroupParser::MSG_EXCEPTION_TOO_MANY_COL_GRPS = "Too many column groups, maximum num of column groups is limited to %d.";
const char * ColumnGroupParser::MSG_EXCEPTION_EMPTY_COL_GRP = "Could not define empty column group.";

/**
 * 解析属性组
 * @param tableDef 表定义 
 * @param valueStr 要解析的字符串
 */
Array<ColGroupDef *> * ColumnGroupParser::parse(const TableDef *tableDef, const char *valueStr) throw(NtseException) {
	AutoPtr<Parser> parser(new Parser(valueStr));
	Array<char *> * bracketsGrpArray = NULL;
	Array<char *> *colNameArray = NULL;
	Array<ColGroupDef *> *colGrpDefArray = NULL;

	try {
		//将各个使用括号的属性组解析出来
		bracketsGrpArray = Parser::parseBracketsGroups(parser->remainingString());

		u16 numColGrps = (u16)bracketsGrpArray->getSize();
		assert(numColGrps > 0);		
		if (numColGrps > ColGroupDef::MAX_NUM_COL_GROUPS)
			NTSE_THROW(NTSE_EC_INVALID_COL_GRP, MSG_EXCEPTION_TOO_MANY_COL_GRPS, ColGroupDef::MAX_NUM_COL_GROUPS);

		colGrpDefArray = new Array<ColGroupDef *>(); //属性组定义

		for (size_t i = 0; i < numColGrps; i++) {
			//解析属性组中的各个列
			colNameArray = Parser::parseList((*bracketsGrpArray)[i], COL_COMMA_DELIMITER);

			ColGroupDefBuilder colGrpDefBuilder((u8)i);
			u16 numCols =  (u16)colNameArray->getSize();
			if (numCols == 0) {
				NTSE_THROW(NTSE_EC_INVALID_COL_GRP, MSG_EXCEPTION_EMPTY_COL_GRP);
			}
			for (u16 j = 0; j <numCols; j++) {
				char *str = (*colNameArray)[j];

				int colNum = -1;
				colNum = tableDef->getColumnNo(str);
				if (colNum < 0) {//检查提供的列名是否有效
					NTSE_THROW(NTSE_EC_INVALID_COL_GRP, MSG_EXCEPTION_NON_EXISTED_COL_NAME, str);
				}
				colGrpDefBuilder.appendCol((u16)colNum);
			}
			colGrpDefArray->push(colGrpDefBuilder.getColGrpDef());

			if (colNameArray) {
				for (size_t j = 0; j < colNameArray->getSize(); j++) {
					delete []((*colNameArray)[j]);
				}
				delete colNameArray;
				colNameArray = NULL;
			}
		}//for (size_t i = 0;...

		if (bracketsGrpArray) {
			for (size_t i = 0; i < numColGrps; i++) {
				delete []((*bracketsGrpArray)[i]);
			}
			delete bracketsGrpArray;
			bracketsGrpArray = NULL;
		}

		return colGrpDefArray;
	} catch (NtseException &e) {
		if (colNameArray) {
			for (size_t i = 0; i < colNameArray->getSize(); i++) {
				delete []((*colNameArray)[i]);
			}
			delete colNameArray;
			colNameArray = NULL;
		}
		if (bracketsGrpArray) {
			for (size_t i = 0; i < bracketsGrpArray->getSize(); i++) {
				delete []((*bracketsGrpArray)[i]);
			}
			delete bracketsGrpArray;
			bracketsGrpArray = NULL;
		}
		if (colGrpDefArray) {
			for (size_t i = 0; i < colGrpDefArray->getSize(); i++)
				delete (*colGrpDefArray)[i];
			delete colGrpDefArray;
		}
		throw e;
	}
}

}