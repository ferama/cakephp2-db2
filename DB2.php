<?php
/**
 * DB2 ibm driver for DBO
 *
 * PHP versions 5
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright     Marco Ferragina
 * @package       datasources
 * @subpackage    datasources.models.datasources.dbo
 * @license       MIT License (http://www.opensource.org/licenses/mit-license.php)
 */

App::uses('DboSource', 'Model/Datasource');


///////////////////// NOTE /////////////////////////////
//
// Insert queries needs journaling, read here:
// http://forums.zend.com/viewtopic.php?f=68&t=8616
//
////////////////////////////////////////////////////////

/**
 * Short description for class.
 *
 * Long description for class
 *
 * @package       cake
 * @subpackage    cake.cake.libs.model.datasources.dbo
 */
class DB2 extends DboSource {

/**
 * Driver description
 *
 * @var string
 */
	public $description = "IBM DB2 DBO Driver";

/**
 * Database keyword used to assign aliases to identifiers.
 *
 * @var string
 */
	public $alias = "";

/**
 * Table/column starting quote
 *
 * @var string
 */
	public $startQuote = "";

/**
 * Table/column end quote
 *
 * @var string
 */
	public $endQuote = "";

/**
 * Columns
 *
 * @var array
 */
	//var $columns = array();
    private $columns = array('primary_key' => array('name' => 'int(11) DEFAULT NULL auto_increment'),
                    'string' => array('type' => 'varchar', 'limit' => '255'),
                    'char' => array('type' => 'char', 'limit' => '255'),
                    'varchar' => array('type' => 'varchar', 'limit' => '255'),
                    'clob' => array('type' => 'text'),
                    'integer' => array('type' => 'int', 'limit' => '11'),
                    'smallint' => array('type' => 'int', 'limit' => '6'),
                    'float' => array('type' => 'float'),
                    'numeric' => array('type' => 'numeric'),
                    'decimal' => array('type' => 'numeric'),
                    'datetime' => array('type' => 'datetime', 'format' => 'Y-m-d h:i:s', 'formatter' => 'date'),
                    'timestamp' => array('type' => 'datetime', 'format' => 'Y-m-d h:i:s', 'formatter' => 'date'),
                    'time' => array('type' => 'time', 'format' => 'h:i:s', 'formatter' => 'date'),
                    'date' => array('type' => 'date', 'format' => 'd/m/Y', 'formatter' => 'date'),
                    'binary' => array('type' => 'blob'),
                    'boolean' => array('type' => 'tinyint', 'limit' => '1'));

/**
 * Whether or not to cache the results of DboSource::name() and DboSource::conditions()
 * into the memory cache.  Set to false to disable the use of the memory cache.
 *
 * @var boolean.
 */
	public $cacheMethods = true;

/**
* Connects to the database using options in the given configuration array.
*
* @return boolean True if the database could be connected, else false
*/
	public function connect() {
		$this->connected = false;
		try {
			$flags = array(
				PDO::ATTR_PERSISTENT => $this->config['persistent'],
				PDO::ATTR_EMULATE_PREPARES => true,
			);
			if (!empty($this->config['encoding'])) {
				//$flags[PDO::MYSQL_ATTR_INIT_COMMAND] = 'SET NAMES ' . $config['encoding'];
			}
			$this->_connection = new PDO(
				"ibm:*LOCAL", "{$this->config['login']}", "{$this->config['password']}", $flags
			);
            $query = "SET CURRENT SCHEMA = {$this->config['database']}";
            $this->_connection->query($query);

			$this->_connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
			$this->connected = true;
		} catch (PDOException $e) {
			throw new MissingConnectionException(array('class' => $e->getMessage()));
		}
		return $this->connected;
	}

/**
 * Check if the ODBC extension is installed/loaded
 *
 * @return boolean
 */
	public function enabled() {
		return in_array('ibm', PDO::getAvailableDrivers());
	}

/**
 * Returns an array of sources (tables) in the database.
 *
 * @return array Array of tablenames in the database
 */
    function listSources() {
        $cache = parent::listSources();
        if ($cache != null) {
            return $cache;
        }

        $result = $this->_connection->query("select * from qsys2.systables where TABLE_SCHEMA = '".$this->config['database']."'");
        $tables = array_map('trim', $result->fetchAll(PDO::FETCH_COLUMN, 0));
        unset($result);
        parent::listSources($tables);
        return $tables;
    }

/**
 * Returns an array of the fields in given table name.
 *
 * @param Model $model Model object to describe
 * @return array Fields in table. Keys are name and type
 */
    function describe($model) {
        $cache = parent::describe($model);
        if ($cache != null) {
            //return $cache;
        }

        $fields = array();
        //$sql = 'SELECT * FROM '.$this->config['database'].'.'.$this->fullTableName($model, false);

        $sql = "select column_name, data_type, length, numeric_scale from qsys2.syscolumns where table_schema = '".$this->config['database']."' and table_name = '".strtoupper($this->fullTableName($model, false))."'";

        $fields = array();
        try{
            $result = $this->_connection->query($sql);
        } catch (PDOException $e) {
            debug($e->getMessage());
        }
        if ($result instanceof PDOStatement == false) {
            return $fields;
        }
        $rows = $result->fetchAll(PDO::FETCH_ASSOC);

        if (!is_array($rows)) {
            return $fields;
        }
        unset($result);

        //$fields['id'] = $this->columns['integer'];

        foreach ($rows as $row) {
            //debug($this->columns);
            $cols = array_keys($row);
            $row['DATA_TYPE'] = strtolower(trim($row['DATA_TYPE']));
            $fields[strtolower($row['COLUMN_NAME'])] = $this->columns[$row['DATA_TYPE']];
        }
        //$fields['ID']['key'] = 'primary';
        $this->_cacheDescription($model->tablePrefix . $model->table, $fields);

        return $fields;
    }

/**
 * Enter description here...
 *
 * @param unknown_type $results
 */
	public function resultSet(&$results) {
		$this->map = array();
		$clean = substr($results->queryString, strpos($results->queryString, " ") + 1);
		$clean = substr($clean, 0, strpos($clean, ' FROM') - strlen($clean));
		$parts = explode(", ", $clean);
		foreach ($parts as $key => $value) {
			list($table, $name) = pluginSplit($value, false, 0);
			if (!$table && strpos($name, $this->virtualFieldSeparator) !== false) {
				$name = substr(strrchr($name, " "), 1);
			}
			$this->map[$key] = array($table, $name, "VAR_STRING");
		}
	}

/**
 * Fetches the next row from the current result set
 *
 * @return unknown
 */

	/**
 * Fetches the next row from the current result set
 *
 * @return mixed array with results fetched and mapped to column names or false if there is no results left to fetch
 */
	public function fetchResult() {
		if ($row = $this->_result->fetch()) {
			$resultRow = array();
			foreach ($this->map as $col => $meta) {
				list($table, $column, $type) = $meta;
				if (strpos($column,'COUNT(')!== false) {
					$column = 'count';
				}
				$resultRow[$table][$column] = trim($row[$col]);
				if ($type === 'boolean' && !is_null($row[$col])) {
					$resultRow[$table][$column] = $this->boolean($resultRow[$table][$column]);
				}
			}
			return $resultRow;
		}
		$this->_result->closeCursor();
		return false;
	}

/**
 * Renders a final SQL statement by putting together the component parts in the correct order
 *
 * @param string $type type of query being run. e.g select, create, update, delete, schema, alter.
 * @param array $data Array of data to insert into the query.
 * @return string Rendered SQL expression to be run.
 */
    public function renderStatement($type, $data) {
        extract($data);

        switch (strtolower($type)) {
            case 'select':
                $sql = "SELECT {$fields} FROM {$table} {$alias} {$joins} {$conditions} {$group} {$order}";
                if (!empty($limit)) {
                    $pieces = explode(", ", $fields);
                    $t = explode(".", $pieces[0]);
                    $objName =  $t[0];
                    $limit_sql = "SELECT {$fields}, {$objName}.CakeRowNum
                                  FROM ( SELECT t.*, row_number() OVER() AS CakeRowNum
                                         FROM (".$sql.") AS t
                                       ) AS {$objName}
                                  WHERE {$objName}.CakeRowNum BETWEEN ".$limit;
                   // debug($limit_sql);
                    return $limit_sql;
                }
                return $sql;
            default:
                return parent::renderStatement($type, $data);
        }
    }



/**
 * Returns a limit statement in the correct format for the particular database.
 *
 * @param integer $limit Limit of results returned
 * @param integer $offset Offset from which to start results
 * @return string SQL limit/offset statement
 */
	public function limit($limit, $offset = null) {
        if ($limit) {
            if (is_null($offset)) {
                return "0 AND $limit";
            }
            else {
                $start = $offset + 1;
                $end = $offset + $limit;
                return "$start AND $end";
            }
        }
		return null;
	}

	/**
 * Returns an SQL calculation, i.e. COUNT() or MAX()
 *
 * @param model $model
 * @param string $func Lowercase name of SQL function, i.e. 'count' or 'max'
 * @param array $params Function parameters (any values must be quoted manually)
 * @return string An SQL calculation function
 */
	public function calculate($model, $func, $params = array()) {
		$params = (array)$params;

		switch (strtolower($func)) {
			case 'count':
				if (!isset($params[0])) {
					$params[0] = '*';
				}
				if (!isset($params[1])) {
					$params[1] = 'count';
				}
				if (is_object($model) && $model->isVirtualField($params[0])){
					$arg = $this->__quoteFields($model->getVirtualField($params[0]));
				} else {
					$arg = $this->name($params[0]);
				}
				return "COUNT($arg) AS $params[1]";
			case 'max':
			case 'min':
				if (!isset($params[1])) {
					$params[1] = $params[0];
				}
				if (is_object($model) && $model->isVirtualField($params[0])) {
					$arg = $this->__quoteFields($model->getVirtualField($params[0]));
				} else {
					$arg = $this->name($params[0]);
				}
				return strtoupper($func) . '(' . $arg . ') AS ' . $this->name($params[1]);
			break;
		}
	}

/**
 * Returns a quoted and escaped string of $data for use in an SQL statement.
 *
 * @param string $data String to be prepared for use in an SQL statement
 * @param string $column The column into which this data will be inserted
 * @return string Quoted and escaped data
 */
	public function value($data, $column = null) {
		if (is_array($data) && !empty($data)) {
			return array_map(
				array(&$this, 'value'),
				$data, array_fill(0, count($data), $column)
			);
		} elseif (is_object($data) && isset($data->type, $data->value)) {
			if ($data->type == 'identifier') {
				return $this->name($data->value);
			} elseif ($data->type == 'expression') {
				return $data->value;
			}
		} elseif (in_array($data, array('{$__cakeID__$}', '{$__cakeForeignKey__$}'), true)) {
			return $data;
		}

		if ($data === null || (is_array($data) && empty($data))) {
			return 'NULL';
		}

		if (empty($column)) {
			$column = $this->introspectType($data);
		}

		switch ($column) {
			case 'binary':
				return $this->_connection->quote($data, PDO::PARAM_LOB);
			break;
			case 'boolean':
				return $this->_connection->quote($this->boolean($data, true), PDO::PARAM_BOOL);
			break;
			case 'string':
			case 'text':
				if ((is_int($data) || $data === '0') || (
					is_numeric($data) && strpos($data, ',') === false &&
					$data[0] != '0' && strpos($data, 'e') === false)
				) {
					//return $data;
				}
				return "'$data'";
			default:
				if ($data === '') {
					return 'NULL';
				}
				if (is_float($data)) {
					return sprintf('%F', $data);
				}
				if ((is_int($data) || $data === '0') || (
					is_numeric($data) && strpos($data, ',') === false &&
					$data[0] != '0' && strpos($data, 'e') === false)
				) {
					//return $data;
				}
				return "'$data'";
			break;
		}
	}

/**
 * Builds and generates an SQL statement from an array.	 Handles final clean-up before conversion.
 *
 * @param array $query An array defining an SQL query
 * @param Model $model The model object which initiated the query
 * @return string An executable SQL statement
 * @see DboSource::renderStatement()
 */
	public function buildStatement($query, $model) {
		$query = array_merge(array('offset' => null, 'joins' => array()), $query);
		if (!empty($query['joins'])) {
			$count = count($query['joins']);
			for ($i = 0; $i < $count; $i++) {
				if (is_array($query['joins'][$i])) {
					$query['conditions'][] = $query['joins'][$i]['conditions'];
				}
			}
		}
		return parent::buildStatement($query, $model);
	}

/**
 * Renders a final SQL JOIN statement
 *
 * @param array $data
 * @return string
 */
	public function renderJoinStatement($data) {
		extract($data);
		return trim(", {$table} {$alias}");
	}

/**
 * Generates the fields list of an SQL query.
 *
 * @param Model $model
 * @param string $alias Alias table name
 * @param mixed $fields
 * @param boolean $quote If false, returns fields array unquoted
 * @return array
 */
	public function fields(Model $model, $alias = null, $fields = array(), $quote = true) {
		if (empty($fields) && !$model->schema(true)) {
			$fields = '*';
		}
		return parent::fields($model, $alias, $fields, $quote);
	}
///**
// * Creates a WHERE clause by parsing given conditions array.  Used by DboSource::conditions().
// *
// * @param array $conditions Array or string of conditions
// * @param boolean $quoteValues If true, values should be quoted
// * @param Model $model A reference to the Model instance making the query
// * @return string SQL fragment
// */
//	public function conditionKeysToString($conditions, $quoteValues = true, $model = null) {
//		$out = array();
//		$data = $columnType = null;
//		$bool = array('and', 'or', 'not', 'and not', 'or not', 'xor', '||', '&&');
//
//		foreach ($conditions as $key => $value) {
//			$join = ' AND ';
//			$not = null;
//
//			if (is_array($value)) {
//				$valueInsert = (
//					!empty($value) &&
//					(substr_count($key, '?') === count($value) || substr_count($key, ':') === count($value))
//				);
//			}
//
//			if (is_numeric($key) && empty($value)) {
//				continue;
//			} elseif (is_numeric($key) && is_string($value)) {
//				$out[] = $not . $this->_quoteFields($value);
//			} elseif ((is_numeric($key) && is_array($value)) || in_array(strtolower(trim($key)), $bool)) {
//				if (in_array(strtolower(trim($key)), $bool)) {
//					$join = ' ' . strtoupper($key) . ' ';
//				} else {
//					$key = $join;
//				}
//				$value = $this->conditionKeysToString($value, $quoteValues, $model);
//
//				if (strpos($join, 'NOT') !== false) {
//					if (strtoupper(trim($key)) === 'NOT') {
//						$key = 'AND ' . trim($key);
//					}
//					$not = 'NOT ';
//				}
//
//				if (empty($value[1])) {
//					if ($not) {
//						$out[] = $not . '(' . $value[0] . ')';
//					} else {
//						$out[] = $value[0] ;
//					}
//				} else {
//					$out[] = '(' . $not . '(' . implode(') ' . strtoupper($key) . ' (', $value) . '))';
//				}
//			} else {
//				if (is_object($value) && isset($value->type)) {
//					if ($value->type === 'identifier') {
//						$data .= $this->name($key) . ' = ' . $this->name($value->value);
//					} elseif ($value->type === 'expression') {
//						if (is_numeric($key)) {
//							$data .= $value->value;
//						} else {
//							$data .= $this->name($key) . ' LIKE ' . $value->value;
//						}
//					}
//				} elseif (is_array($value) && !empty($value) && !$valueInsert) {
//					$keys = array_keys($value);
//					if ($keys === array_values($keys)) {
//						$count = count($value);
//						if ($count === 1) {
//							$data = $this->_quoteFields($key) . ' LIKE ';
//							$close = false;
//						} else {
//							$data = $this->_quoteFields($key) . ' IN (';
//						}
//						if ($quoteValues) {
//							if (is_object($model)) {
//								$columnType = $model->getColumnType($key);
//							}
//							$data .= implode(', ', $this->value($value, $columnType));
//						}
//						if (!empty($close)) {
//							$data .= ')';
//						}
//					} else {
//						$ret = $this->conditionKeysToString($value, $quoteValues, $model);
//						if (count($ret) > 1) {
//							$data = '(' . implode(') AND (', $ret) . ')';
//						} elseif (isset($ret[0])) {
//							$data = $ret[0];
//						}
//					}
//				} elseif (is_numeric($key) && !empty($value)) {
//					$data = $this->_quoteFields($value);
//				} else {
//					$data = $this->_parseKey($model, trim($key), $value);
//				}
//
//				if ($data != null) {
//					$out[] = $data;
//					$data = null;
//				}
//			}
//		}
//		return $out;
//	}
//
///**
// * Extracts a Model.field identifier and an SQL condition operator from a string, formats
// * and inserts values, and composes them into an SQL snippet.
// *
// * @param Model $model Model object initiating the query
// * @param string $key An SQL key snippet containing a field and optional SQL operator
// * @param mixed $value The value(s) to be inserted in the string
// * @return string
// */
//	protected function _parseKey($model, $key, $value) {
//		$operatorMatch = '/^(((' . implode(')|(', $this->_sqlOps);
//		$operatorMatch .= ')\\x20?)|<[>=]?(?![^>]+>)\\x20?|[>=!]{1,3}(?!<)\\x20?)/is';
//		$bound = (strpos($key, '?') !== false || (is_array($value) && strpos($key, ':') !== false));
//
//		if (strpos($key, ' ') === false) {
//			$operator = 'LIKE';
//		} else {
//			list($key, $operator) = explode(' ', trim($key), 2);
//
//			if (!preg_match($operatorMatch, trim($operator)) && strpos($operator, ' ') !== false) {
//				$key = $key . ' ' . $operator;
//				$split = strrpos($key, ' ');
//				$operator = substr($key, $split);
//				$key = substr($key, 0, $split);
//			}
//		}
//
//		$virtual = false;
//		if (is_object($model) && $model->isVirtualField($key)) {
//			$key = $this->_quoteFields($model->getVirtualField($key));
//			$virtual = true;
//		}
//
//		$type = is_object($model) ? $model->getColumnType($key) : null;
//		$null = $value === null || (is_array($value) && empty($value));
//
//		if (strtolower($operator) === 'not') {
//			$data = $this->conditionKeysToString(
//				array($operator => array($key => $value)), true, $model
//			);
//			return $data[0];
//		}
//
//		$value = $this->value($value, $type);
//
//		if (!$virtual && $key !== '?') {
//			$isKey = (strpos($key, '(') !== false || strpos($key, ')') !== false);
//			$key = $isKey ? $this->_quoteFields($key) : $this->name($key);
//		}
//
//		if ($bound) {
//			return String::insert($key . ' ' . trim($operator), $value);
//		}
//
//		if (!preg_match($operatorMatch, trim($operator))) {
//			$operator .= ' LIKE';
//		}
//		$operator = trim($operator);
//
//		if (is_array($value)) {
//			$value = implode(', ', $value);
//
//			switch ($operator) {
//				case '=':
//					$operator = 'IN';
//				break;
//				case '!=':
//				case '<>':
//					$operator = 'NOT IN';
//				break;
//			}
//			$value = "({$value})";
//		} elseif ($null || $value === 'NULL') {
//			switch ($operator) {
//				case '=':
//					$operator = 'IS';
//				break;
//				case '!=':
//				case '<>':
//					$operator = 'IS NOT';
//				break;
//			}
//		}
//		if ($virtual) {
//			return "({$key}) {$operator} {$value}";
//		}
//		return "{$key} {$operator} {$value}";
//	}

}
