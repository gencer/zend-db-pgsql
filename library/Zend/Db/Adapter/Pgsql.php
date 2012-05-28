<?php
/**
 * Zend Framework, PgSQL Adapter
 *
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Adapter
 * @copyright  Jenei Viktor Attila // Fixed By Gencer Genç
 * @license   http://framework.zend.com/license/new-bsd     New BSD License
 */

/**
 * @see Zend_Db_Adapter_Abstract
 */
require_once 'Zend/Db/Adapter/Abstract.php';

/**
 * @see Zend_Db_Profiler
 */
require_once 'Zend/Db/Profiler.php';

/**
 * @see Zend_Db_Select
 */
require_once 'Zend/Db/Select.php';

/**
 * @see Zend_Db_Statement_Mysqli
 */
require_once 'Zend/Db/Statement/Pgsql.php';


/**
 * @category   Zend
 * @package    Zend_Db
 * @subpackage Adapter
 * @copyright  Jenei Viktor Attila
 * @license   http://framework.zend.com/license/new-bsd     New BSD License
 */
class Zend_Db_Adapter_Pgsql extends Zend_Db_Adapter_Abstract {
    /**
     * Keys are UPPERCASE SQL datatypes or the constants
     * Zend_Db::INT_TYPE, Zend_Db::BIGINT_TYPE, or Zend_Db::FLOAT_TYPE.
     *
     * Values are:
     * 0 = 32-bit integer
     * 1 = 64-bit integer
     * 2 = float or decimal
     *
     * @var array Associative array of datatypes to values 0, 1, or 2.
     */
    protected $_numericDataTypes = array(
        Zend_Db::INT_TYPE    => Zend_Db::INT_TYPE,
        Zend_Db::BIGINT_TYPE => Zend_Db::BIGINT_TYPE,
        Zend_Db::FLOAT_TYPE  => Zend_Db::FLOAT_TYPE,
        'INTEGER'            => Zend_Db::INT_TYPE,
        'SERIAL'             => Zend_Db::INT_TYPE,
        'SMALLINT'           => Zend_Db::INT_TYPE,
        'BIGINT'             => Zend_Db::BIGINT_TYPE,
        'BIGSERIAL'          => Zend_Db::BIGINT_TYPE,
        'DECIMAL'            => Zend_Db::FLOAT_TYPE,
        'DOUBLE PRECISION'   => Zend_Db::FLOAT_TYPE,
        'NUMERIC'            => Zend_Db::FLOAT_TYPE,
        'REAL'               => Zend_Db::FLOAT_TYPE
    );

    /**
     * @var Zend_Db_Statement_Mysqli
     */
    protected $_stmt = null;
    
    /**
     * @var LastInsertId when possible
     */
    protected $lastid = null;

    /**
     * Default class name for a DB statement.
     *
     * @var string
     */
    protected $_defaultStmtClass = 'Zend_Db_Statement_Pgsql';

    /**
     * Quote a raw string.
     *
     * @param mixed $value Raw string
     *
     * @return string           Quoted string
     */
    public $_isConnected;
    
    protected function _quote( $value ) {
        if ( is_int( $value ) || is_float( $value ) ) {
            return $value;
        }
        $this->_connect();
        return "'" . pg_escape_string( $this->_connection, $value ) . "'";
    }

    /**
     * Returns the symbol the adapter uses for delimiting identifiers.
     *
     * @return string
     */
    public function getQuoteIdentifierSymbol() {
        return '"';
    }

    /**
     * Returns a list of the tables in the database.
     *
     * @return array
     */
    public function listTables() {
        $sql = "SELECT c.relname AS table_name "
             . "FROM pg_class c, pg_user u "
             . "WHERE c.relowner = u.usesysid AND c.relkind = 'r' "
             . "AND NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = c.relname) "
             . "AND c.relname !~ '^(pg_|sql_)' "
             . "UNION "
             . "SELECT c.relname AS table_name "
             . "FROM pg_class c "
             . "WHERE c.relkind = 'r' "
             . "AND NOT EXISTS (SELECT 1 FROM pg_views WHERE viewname = c.relname) "
             . "AND NOT EXISTS (SELECT 1 FROM pg_user WHERE usesysid = c.relowner) "
             . "AND c.relname !~ '^pg_'";

        return $this->fetchCol($sql);
    }

    /**
     * Returns the column descriptions for a table.
     *
     * The return value is an associative array keyed by the column name,
     * as returned by the RDBMS.
     *
     * The value of each array element is an associative array
     * with the following keys:
     *
     * SCHEMA_NAME      => string; name of database or schema
     * TABLE_NAME       => string;
     * COLUMN_NAME      => string; column name
     * COLUMN_POSITION  => number; ordinal position of column in table
     * DATA_TYPE        => string; SQL datatype name of column
     * DEFAULT          => string; default expression of column, null if none
     * NULLABLE         => boolean; true if column can have nulls
     * LENGTH           => number; length of CHAR/VARCHAR
     * SCALE            => number; scale of NUMERIC/DECIMAL
     * PRECISION        => number; precision of NUMERIC/DECIMAL
     * UNSIGNED         => boolean; unsigned property of an integer type
     * PRIMARY          => boolean; true if column is part of the primary key
     * PRIMARY_POSITION => integer; position of column in primary key
     * IDENTITY         => integer; true if column is auto-generated with unique values
     *
     * @param string $tableName
     * @param string $schemaName OPTIONAL
     * @return array
     */
    public function describeTable($tableName, $schemaName = null) {
        $sql = "SELECT
                a.attnum,
                n.nspname,
                c.relname,
                a.attname AS colname,
                t.typname AS type,
                a.atttypmod,
                FORMAT_TYPE(a.atttypid, a.atttypmod) AS complete_type,
                d.adsrc AS default_value,
                a.attnotnull AS notnull,
                a.attlen AS length,
                co.contype,
                ARRAY_TO_STRING(co.conkey, ',') AS conkey
            FROM pg_attribute AS a
                JOIN pg_class AS c ON a.attrelid = c.oid
                JOIN pg_namespace AS n ON c.relnamespace = n.oid
                JOIN pg_type AS t ON a.atttypid = t.oid
                LEFT OUTER JOIN pg_constraint AS co ON (co.conrelid = c.oid
                    AND a.attnum = ANY(co.conkey) AND co.contype = 'p')
                LEFT OUTER JOIN pg_attrdef AS d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE a.attnum > 0 AND c.relname = ".$this->quote($tableName);
        if ($schemaName) {
            $sql .= " AND n.nspname = ".$this->quote($schemaName);
        }
        $sql .= ' ORDER BY a.attnum';

        $stmt = $this->query($sql);

        // Use FETCH_NUM so we are not dependent on the CASE attribute of the PDO connection
        $result = $stmt->fetchAll(Zend_Db::FETCH_NUM);

        $attnum        = 0;
        $nspname       = 1;
        $relname       = 2;
        $colname       = 3;
        $type          = 4;
        $atttypemod    = 5;
        $complete_type = 6;
        $default_value = 7;
        $notnull       = 8;
        $length        = 9;
        $contype       = 10;
        $conkey        = 11;

        $desc = array();
        foreach ($result as $key => $row) {
            $defaultValue = $row[$default_value];
            if ($row[$type] == 'varchar' || $row[$type] == 'bpchar' ) {
                if (preg_match('/character(?: varying)?(?:\((\d+)\))?/', $row[$complete_type], $matches)) {
                    if (isset($matches[1])) {
                        $row[$length] = $matches[1];
                    } else {
                        $row[$length] = null; // unlimited
                    }
                }
                if (preg_match("/^'(.*?)'::(?:character varying|bpchar)$/", $defaultValue, $matches)) {
                    $defaultValue = $matches[1];
                }
            }
            list($primary, $primaryPosition, $identity) = array(false, null, false);
            if ($row[$contype] == 'p') {
                $primary = true;
                $primaryPosition = array_search($row[$attnum], explode(',', $row[$conkey])) + 1;
                $identity = (bool) (preg_match('/^nextval/', $row[$default_value]));
            }
            $desc[$this->foldCase($row[$colname])] = array(
                'SCHEMA_NAME'      => $this->foldCase($row[$nspname]),
                'TABLE_NAME'       => $this->foldCase($row[$relname]),
                'COLUMN_NAME'      => $this->foldCase($row[$colname]),
                'COLUMN_POSITION'  => $row[$attnum],
                'DATA_TYPE'        => $row[$type],
                'DEFAULT'          => $defaultValue,
                'NULLABLE'         => (bool) ($row[$notnull] != 't'),
                'LENGTH'           => $row[$length],
                'SCALE'            => null, // @todo
                'PRECISION'        => null, // @todo
                'UNSIGNED'         => null, // @todo
                'PRIMARY'          => $primary,
                'PRIMARY_POSITION' => $primaryPosition,
                'IDENTITY'         => $identity
            );
        }
        return $desc; 
    }

    /**
     * Creates a connection to the database.
     *
     * @return void
     * @throws Zend_Db_Adapter_Exception
     */
    protected function _connect() {
        if ( $this->_connection ) {
            return;
        }

        if ( !extension_loaded( 'pgsql' ) ) {
            /**
             * @see Zend_Db_Adapter_Exception
             */
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception( 'The pgsql extension is required for this adapter but the extension is not loaded' );
        }

        if ( isset($this->_config['port'] ) ) {
            $port = (integer) $this->_config['port'];
        } else {
            $port = 5432;
        }

        
        $strconn = vsprintf( "host=%s port=%s dbname=%s user=%s password=%s", array( $this->_config['host'], $port, $this->_config['dbname'], $this->_config['username'], $this->_config['password'] ) );
        $this->_connection = pg_connect( $strconn );
        
        $_isConnected = true;
        if ( !$this->_connection ) {  $_isConnected = false; }

        if ( $_isConnected === false ) {

            $this->closeConnection();
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception( pg_last_error( $this->_connection ) );
        }

        if ( !empty( $this->_config['charset'] ) ) {
            $sql_query = "SET NAMES '" . $this->_config['charset'] . "'";
            @pg_query( $this->_connection, $sql_query );
        }
    }

    /**
     * Test if a connection is active
     *
     * @return boolean
     */
    public function isConnected() {
        return $this->_isConnected;
    }

    /**
     * Force the connection to close.
     *
     * @return void
     */
    public function closeConnection() {
        if ( $this->isConnected() ) {
            @pg_close( $this->_connection );
        }
        $this->_connection = null;
        $this->_isConnected = false;
    }

    /**
     * Prepare a statement and return a PDOStatement-like object.
     *
     * @param  string  $sql  SQL query
     * @return Zend_Db_Statement_Mysqli
     */
    public function prepare( $sql ) {
        $this->_connect();
        if ( $this->_stmt ) {
            $this->_stmt->close();
        }
        $stmtClass = $this->_defaultStmtClass;
        if ( !class_exists( $stmtClass ) ) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass( $stmtClass );
        }
        $stmt = new $stmtClass( $this, $sql );
        if ( $stmt === false ) {
            return false;
        }

        $stmt->setFetchMode( $this->_fetchMode );
        $this->_stmt = $stmt;
        return $stmt;
    }

    /**
     * Gets the last ID generated automatically by an IDENTITY/AUTOINCREMENT column.
     *
     * As a convention, on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2), this method forms the name of a sequence
     * from the arguments and returns the last id generated by that sequence.
     * On RDBMS brands that support IDENTITY/AUTOINCREMENT columns, this method
     * returns the last value generated for such a column, and the table name
     * argument is disregarded.
     *
     * MySQL does not support sequences, so $tableName and $primaryKey are ignored.
     *
     * @param string $tableName   OPTIONAL Name of table.
     * @param string $primaryKey  OPTIONAL Name of primary key column.
     * @return string
     * @todo Return value should be int?
     */
    public function lastInsertId( $tableName = null, $primaryKey = null ) {
        if ( $tableName !== null ) {
            $sequenceName = $tableName;
            if ($primaryKey) {
                $sequenceName .= "_$primaryKey";
            }
            $sequenceName .= '_seq';
            return $this->lastSequenceId( $sequenceName );
        }
        /* I hope this is correct (after insert returning OID and save it) */
        return $this->lastid;
    }

    /**
     * Begin a transaction.
     *
     * @return void
     */
    protected function _beginTransaction() {
        $this->_connect();
        @pg_query( $this->_connection, 'BEGIN;' );
    }

    /**
     * Commit a transaction.
     *
     * @return void
     */
    protected function _commit() {
        $this->_connect();
        @pg_query( $this->_connection, 'COMMIT;' );
    }

    /**
     * Roll-back a transaction.
     *
     * @return void
     */
    protected function _rollBack() {
    	$this->_connect();
        @pg_query( $this->_connection, 'ROLLBACK;' );
    }

    /**
     * Set the fetch mode.
     *
     * @param int $mode
     * @return void
     * @throws Zend_Db_Adapter_Exception
     */
    public function setFetchMode( $mode ) {
        switch ( $mode ) {
            case Zend_Db::FETCH_LAZY:
            case Zend_Db::FETCH_ASSOC:
            case Zend_Db::FETCH_NUM:
            case Zend_Db::FETCH_BOTH:
            case Zend_Db::FETCH_NAMED:
            case Zend_Db::FETCH_OBJ:
                $this->_fetchMode = $mode;
                break;
            case Zend_Db::FETCH_BOUND:
                /**
                 * @see Zend_Db_Adapter_Exception
                 */
                require_once 'Zend/Db/Adapter/Exception.php';
                throw new Zend_Db_Adapter_Exception( 'FETCH_BOUND is not supported yet' );
                break;
            default:
                /**
                 * @see Zend_Db_Adapter_Exception
                 */
                require_once 'Zend/Db/Adapter/Exception.php';
                throw new Zend_Db_Adapter_Exception( "Invalid fetch mode '$mode' specified" );
        }
    }

    /**
     * Adds an adapter-specific LIMIT clause to the SELECT statement.
     *
     * @param string $sql
     * @param int $count
     * @param int $offset OPTIONAL
     * @return string
     */
    public function limit( $sql, $count, $offset = 0 ) {
        $count = intval($count);
        if ($count <= 0) {
            /**
             * @see Zend_Db_Adapter_Exception
             */
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception("LIMIT argument count=$count is not valid");
        }

        $offset = intval($offset);
        if ($offset < 0) {
            /**
             * @see Zend_Db_Adapter_Exception
             */
            require_once 'Zend/Db/Adapter/Exception.php';
            throw new Zend_Db_Adapter_Exception("LIMIT argument offset=$offset is not valid");
        }

        $sql .= " LIMIT $count";
        if ($offset > 0) {
            $sql .= " OFFSET $offset";
        }

        return $sql;
    }

    /**
     * Return the most recent value from the specified sequence in the database.
     * This is supported only on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2).  Other RDBMS brands return null.
     *
     * @param string $sequenceName
     * @return string
     */
    public function lastSequenceId($sequenceName)
    {
        $this->_connect();
        $value = $this->fetchOne("SELECT CURRVAL(".$this->quote($sequenceName).")");
        return $value;
    }

    /**
     * Generate a new value from the specified sequence in the database, and return it.
     * This is supported only on RDBMS brands that support sequences
     * (e.g. Oracle, PostgreSQL, DB2).  Other RDBMS brands return null.
     *
     * @param string $sequenceName
     * @return string
     */
    public function nextSequenceId($sequenceName)
    {
        $this->_connect();
        $value = $this->fetchOne("SELECT NEXTVAL(".$this->quote($sequenceName).")");
        return $value;
    }
    
    public function getPrimaryKeyName( $tablename ) {
    	$pkey = 'oid';
    	if ( $tablename != '' ) {
            $fields = $this->describeTable( $tablename );
            foreach( $fields as $fname=> $desc ) {
                if ( $desc[ 'PRIMARY' ] == 1 ) {
                    $pkey = $fname;
                    break;
                }
            }
        }
        return $pkey;
    }

    /**
     * Inserts a table row with specified data.
     *
     * @param mixed $table The table to insert data into.
     * @param array $bind Column-value pairs.
     * @return int The number of affected rows.
     */
    public function insert($table, array $bind)
    {
        // extract and quote col names from the array keys
        $cols = array();
        $vals = array();
        foreach ($bind as $col => $val) {
            $cols[] = $this->quoteIdentifier($col, true);
            if ($val instanceof Zend_Db_Expr) {
                $vals[] = $val->__toString();
                unset($bind[$col]);
            } else {
                $vals[] = '?';
            }
        }
        // build the statement
        $pkey = $this->getPrimaryKeyName( $table );
        $sql = 'INSERT INTO '
             . $this->quoteIdentifier($table, true)
             . ' (' . implode(', ', $cols) . ') '
             . 'VALUES (' . implode(', ', $vals) . ') '
             . 'RETURNING ' . $pkey;

        // execute the statement and return the number of affected rows
        $stmt = $this->query( $sql, array_values( $bind ) );
        $result = $stmt->rowCount();
        $pkeyres = $stmt->fetch( Zend_Db::FETCH_NUM );
        $this->lastid = $pkeyres[0];
        return $this->lastid;
    }

    /**
     * Check if the adapter supports real SQL parameters.
     *
     * @param string $type 'positional' or 'named'
     * @return bool
     */
    public function supportsParameters( $type ) {
        switch ( $type ) {
            case 'positional':
                return true;
            case 'named':
            default:
                return false;
        }
    }

    /**
     * Retrieve server version in PHP style
     *
     *@return string
     */
    public function getServerVersion() {
        return null;
    } 
}
?>