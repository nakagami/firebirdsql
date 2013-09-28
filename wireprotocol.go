/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2013 Hajime Nakagami

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*******************************************************************************/

package firebirdsql

import (
    "os"
    "errors"
    "net"
    "bytes"
    "strings"
    "container/list"
)


var errmsgs = map[int]string {
    335544321 : "arithmetic exception, numeric overflow, or string truncation\n", 
    335544322 : "invalid database key\n", 
    335544323 : "file @1 is not a valid database\n", 
    335544324 : "invalid database handle (no active connection)\n", 
    335544325 : "bad parameters on attach or create database\n", 
    335544326 : "unrecognized database parameter block\n", 
    335544327 : "invalid request handle\n", 
    335544328 : "invalid BLOB handle\n", 
    335544329 : "invalid BLOB ID\n", 
    335544330 : "invalid parameter in transaction parameter block\n", 
    335544331 : "invalid format for transaction parameter block\n", 
    335544332 : "invalid transaction handle (expecting explicit transaction start)\n", 
    335544333 : "internal Firebird consistency check (@1)\n", 
    335544334 : "conversion error from string \"@1\"\n", 
    335544335 : "database file appears corrupt (@1)\n", 
    335544336 : "deadlock\n", 
    335544337 : "attempt to start more than @1 transactions\n", 
    335544338 : "no match for first value expression\n", 
    335544339 : "information type inappropriate for object specified\n", 
    335544340 : "no information of this type available for object specified\n", 
    335544341 : "unknown information item\n", 
    335544342 : "action cancelled by trigger (@1) to preserve data integrity\n", 
    335544343 : "invalid request BLR at offset @1\n", 
    335544344 : "I/O error during \"@1\" operation for file \"@2\"\n", 
    335544345 : "lock conflict on no wait transaction\n", 
    335544346 : "corrupt system table\n", 
    335544347 : "validation error for column @1, value \"@2\"\n", 
    335544348 : "no current record for fetch operation\n", 
    335544349 : "attempt to store duplicate value (visible to active transactions) in unique index \"@1\"\n", 
    335544350 : "program attempted to exit without finishing database\n", 
    335544351 : "unsuccessful metadata update\n", 
    335544352 : "no permission for @1 access to @2 @3\n", 
    335544353 : "transaction is not in limbo\n", 
    335544354 : "invalid database key\n", 
    335544355 : "BLOB was not closed\n", 
    335544356 : "metadata is obsolete\n", 
    335544357 : "cannot disconnect database with open transactions (@1 active)\n", 
    335544358 : "message length error (encountered @1, expected @2)\n", 
    335544359 : "attempted update of read-only column\n", 
    335544360 : "attempted update of read-only table\n", 
    335544361 : "attempted update during read-only transaction\n", 
    335544362 : "cannot update read-only view @1\n", 
    335544363 : "no transaction for request\n", 
    335544364 : "request synchronization error\n", 
    335544365 : "request referenced an unavailable database\n", 
    335544366 : "segment buffer length shorter than expected\n", 
    335544367 : "attempted retrieval of more segments than exist\n", 
    335544368 : "attempted invalid operation on a BLOB\n", 
    335544369 : "attempted read of a new, open BLOB\n", 
    335544370 : "attempted action on BLOB outside transaction\n", 
    335544371 : "attempted write to read-only BLOB\n", 
    335544372 : "attempted reference to BLOB in unavailable database\n", 
    335544373 : "operating system directive @1 failed\n", 
    335544374 : "attempt to fetch past the last record in a record stream\n", 
    335544375 : "unavailable database\n", 
    335544376 : "table @1 was omitted from the transaction reserving list\n", 
    335544377 : "request includes a DSRI extension not supported in this implementation\n", 
    335544378 : "feature is not supported\n", 
    335544379 : "unsupported on-disk structure for file @1; found @2.@3, support @4.@5\n", 
    335544380 : "wrong number of arguments on call\n", 
    335544381 : "Implementation limit exceeded\n", 
    335544382 : "@1\n", 
    335544383 : "unrecoverable conflict with limbo transaction @1\n", 
    335544384 : "internal error\n", 
    335544385 : "internal error\n", 
    335544386 : "too many requests\n", 
    335544387 : "internal error\n", 
    335544388 : "block size exceeds implementation restriction\n", 
    335544389 : "buffer exhausted\n", 
    335544390 : "BLR syntax error: expected @1 at offset @2, encountered @3\n", 
    335544391 : "buffer in use\n", 
    335544392 : "internal error\n", 
    335544393 : "request in use\n", 
    335544394 : "incompatible version of on-disk structure\n", 
    335544395 : "table @1 is not defined\n", 
    335544396 : "column @1 is not defined in table @2\n", 
    335544397 : "internal error\n", 
    335544398 : "internal error\n", 
    335544399 : "internal error\n", 
    335544400 : "internal error\n", 
    335544401 : "internal error\n", 
    335544402 : "internal error\n", 
    335544403 : "page @1 is of wrong type (expected @2, found @3)\n", 
    335544404 : "database corrupted\n", 
    335544405 : "checksum error on database page @1\n", 
    335544406 : "index is broken\n", 
    335544407 : "database handle not zero\n", 
    335544408 : "transaction handle not zero\n", 
    335544409 : "transaction--request mismatch (synchronization error)\n", 
    335544410 : "bad handle count\n", 
    335544411 : "wrong version of transaction parameter block\n", 
    335544412 : "unsupported BLR version (expected @1, encountered @2)\n", 
    335544413 : "wrong version of database parameter block\n", 
    335544414 : "BLOB and array data types are not supported for @1 operation\n", 
    335544415 : "database corrupted\n", 
    335544416 : "internal error\n", 
    335544417 : "internal error\n", 
    335544418 : "transaction in limbo\n", 
    335544419 : "transaction not in limbo\n", 
    335544420 : "transaction outstanding\n", 
    335544421 : "connection rejected by remote interface\n", 
    335544422 : "internal error\n", 
    335544423 : "internal error\n", 
    335544424 : "no lock manager available\n", 
    335544425 : "context already in use (BLR error)\n", 
    335544426 : "context not defined (BLR error)\n", 
    335544427 : "data operation not supported\n", 
    335544428 : "undefined message number\n", 
    335544429 : "bad parameter number\n", 
    335544430 : "unable to allocate memory from operating system\n", 
    335544431 : "blocking signal has been received\n", 
    335544432 : "lock manager error\n", 
    335544433 : "communication error with journal \"@1\"\n", 
    335544434 : "key size exceeds implementation restriction for index \"@1\"\n", 
    335544435 : "null segment of UNIQUE KEY\n", 
    335544436 : "SQL error code = @1\n", 
    335544437 : "wrong DYN version\n", 
    335544438 : "function @1 is not defined\n", 
    335544439 : "function @1 could not be matched\n", 
    335544440 : "\n", 
    335544441 : "database detach completed with errors\n", 
    335544442 : "database system cannot read argument @1\n", 
    335544443 : "database system cannot write argument @1\n", 
    335544444 : "operation not supported\n", 
    335544445 : "@1 extension error\n", 
    335544446 : "not updatable\n", 
    335544447 : "no rollback performed\n", 
    335544448 : "\n", 
    335544449 : "\n", 
    335544450 : "@1\n", 
    335544451 : "update conflicts with concurrent update\n", 
    335544452 : "product @1 is not licensed\n", 
    335544453 : "object @1 is in use\n", 
    335544454 : "filter not found to convert type @1 to type @2\n", 
    335544455 : "cannot attach active shadow file\n", 
    335544456 : "invalid slice description language at offset @1\n", 
    335544457 : "subscript out of bounds\n", 
    335544458 : "column not array or invalid dimensions (expected @1, encountered @2)\n", 
    335544459 : "record from transaction @1 is stuck in limbo\n", 
    335544460 : "a file in manual shadow @1 is unavailable\n", 
    335544461 : "secondary server attachments cannot validate databases\n", 
    335544462 : "secondary server attachments cannot start journaling\n", 
    335544463 : "generator @1 is not defined\n", 
    335544464 : "secondary server attachments cannot start logging\n", 
    335544465 : "invalid BLOB type for operation\n", 
    335544466 : "violation of FOREIGN KEY constraint \"@1\" on table \"@2\"\n", 
    335544467 : "minor version too high found @1 expected @2\n", 
    335544468 : "transaction @1 is @2\n", 
    335544469 : "transaction marked invalid by I/O error\n", 
    335544470 : "cache buffer for page @1 invalid\n", 
    335544471 : "there is no index in table @1 with id @2\n", 
    335544472 : "Your user name and password are not defined. Ask your database administrator to set up a Firebird login.\n", 
    335544473 : "invalid bookmark handle\n", 
    335544474 : "invalid lock level @1\n", 
    335544475 : "lock on table @1 conflicts with existing lock\n", 
    335544476 : "requested record lock conflicts with existing lock\n", 
    335544477 : "maximum indexes per table (@1) exceeded\n", 
    335544478 : "enable journal for database before starting online dump\n", 
    335544479 : "online dump failure. Retry dump\n", 
    335544480 : "an online dump is already in progress\n", 
    335544481 : "no more disk/tape space.  Cannot continue online dump\n", 
    335544482 : "journaling allowed only if database has Write-ahead Log\n", 
    335544483 : "maximum number of online dump files that can be specified is 16\n", 
    335544484 : "error in opening Write-ahead Log file during recovery\n", 
    335544485 : "invalid statement handle\n", 
    335544486 : "Write-ahead log subsystem failure\n", 
    335544487 : "WAL Writer error\n", 
    335544488 : "Log file header of @1 too small\n", 
    335544489 : "Invalid version of log file @1\n", 
    335544490 : "Log file @1 not latest in the chain but open flag still set\n", 
    335544491 : "Log file @1 not closed properly; database recovery may be required\n", 
    335544492 : "Database name in the log file @1 is different\n", 
    335544493 : "Unexpected end of log file @1 at offset @2\n", 
    335544494 : "Incomplete log record at offset @1 in log file @2\n", 
    335544495 : "Log record header too small at offset @1 in log file @2\n", 
    335544496 : "Log block too small at offset @1 in log file @2\n", 
    335544497 : "Illegal attempt to attach to an uninitialized WAL segment for @1\n", 
    335544498 : "Invalid WAL parameter block option @1\n", 
    335544499 : "Cannot roll over to the next log file @1\n", 
    335544500 : "database does not use Write-ahead Log\n", 
    335544501 : "cannot drop log file when journaling is enabled\n", 
    335544502 : "reference to invalid stream number\n", 
    335544503 : "WAL subsystem encountered error\n", 
    335544504 : "WAL subsystem corrupted\n", 
    335544505 : "must specify archive file when enabling long term journal for databases with round-robin log files\n", 
    335544506 : "database @1 shutdown in progress\n", 
    335544507 : "refresh range number @1 already in use\n", 
    335544508 : "refresh range number @1 not found\n", 
    335544509 : "CHARACTER SET @1 is not defined\n", 
    335544510 : "lock time-out on wait transaction\n", 
    335544511 : "procedure @1 is not defined\n", 
    335544512 : "Input parameter mismatch for procedure @1\n", 
    335544513 : "Database @1: WAL subsystem bug for pid @2@3\n", 
    335544514 : "Could not expand the WAL segment for database @1\n", 
    335544515 : "status code @1 unknown\n", 
    335544516 : "exception @1 not defined\n", 
    335544517 : "exception @1\n", 
    335544518 : "restart shared cache manager\n", 
    335544519 : "invalid lock handle\n", 
    335544520 : "long-term journaling already enabled\n", 
    335544521 : "Unable to roll over please see Firebird log.\n", 
    335544522 : "WAL I/O error.  Please see Firebird log.\n", 
    335544523 : "WAL writer - Journal server communication error.  Please see Firebird log.\n", 
    335544524 : "WAL buffers cannot be increased.  Please see Firebird log.\n", 
    335544525 : "WAL setup error.  Please see Firebird log.\n", 
    335544526 : "obsolete\n", 
    335544527 : "Cannot start WAL writer for the database @1\n", 
    335544528 : "database @1 shutdown\n", 
    335544529 : "cannot modify an existing user privilege\n", 
    335544530 : "Cannot delete PRIMARY KEY being used in FOREIGN KEY definition.\n", 
    335544531 : "Column used in a PRIMARY constraint must be NOT NULL.\n", 
    335544532 : "Name of Referential Constraint not defined in constraints table.\n", 
    335544533 : "Non-existent PRIMARY or UNIQUE KEY specified for FOREIGN KEY.\n", 
    335544534 : "Cannot update constraints (RDB$REF_CONSTRAINTS).\n", 
    335544535 : "Cannot update constraints (RDB$CHECK_CONSTRAINTS).\n", 
    335544536 : "Cannot delete CHECK constraint entry (RDB$CHECK_CONSTRAINTS)\n", 
    335544537 : "Cannot delete index segment used by an Integrity Constraint\n", 
    335544538 : "Cannot update index segment used by an Integrity Constraint\n", 
    335544539 : "Cannot delete index used by an Integrity Constraint\n", 
    335544540 : "Cannot modify index used by an Integrity Constraint\n", 
    335544541 : "Cannot delete trigger used by a CHECK Constraint\n", 
    335544542 : "Cannot update trigger used by a CHECK Constraint\n", 
    335544543 : "Cannot delete column being used in an Integrity Constraint.\n", 
    335544544 : "Cannot rename column being used in an Integrity Constraint.\n", 
    335544545 : "Cannot update constraints (RDB$RELATION_CONSTRAINTS).\n", 
    335544546 : "Cannot define constraints on views\n", 
    335544547 : "internal Firebird consistency check (invalid RDB$CONSTRAINT_TYPE)\n", 
    335544548 : "Attempt to define a second PRIMARY KEY for the same table\n", 
    335544549 : "cannot modify or erase a system trigger\n", 
    335544550 : "only the owner of a table may reassign ownership\n", 
    335544551 : "could not find table/procedure for GRANT\n", 
    335544552 : "could not find column for GRANT\n", 
    335544553 : "user does not have GRANT privileges for operation\n", 
    335544554 : "table/procedure has non-SQL security class defined\n", 
    335544555 : "column has non-SQL security class defined\n", 
    335544556 : "Write-ahead Log without shared cache configuration not allowed\n", 
    335544557 : "database shutdown unsuccessful\n", 
    335544558 : "Operation violates CHECK constraint @1 on view or table @2\n", 
    335544559 : "invalid service handle\n", 
    335544560 : "database @1 shutdown in @2 seconds\n", 
    335544561 : "wrong version of service parameter block\n", 
    335544562 : "unrecognized service parameter block\n", 
    335544563 : "service @1 is not defined\n", 
    335544564 : "long-term journaling not enabled\n", 
    335544565 : "Cannot transliterate character between character sets\n", 
    335544566 : "WAL defined; Cache Manager must be started first\n", 
    335544567 : "Overflow log specification required for round-robin log\n", 
    335544568 : "Implementation of text subtype @1 not located.\n", 
    335544569 : "Dynamic SQL Error\n", 
    335544570 : "Invalid command\n", 
    335544571 : "Data type for constant unknown\n", 
    335544572 : "Invalid cursor reference\n", 
    335544573 : "Data type unknown\n", 
    335544574 : "Invalid cursor declaration\n", 
    335544575 : "Cursor @1 is not updatable\n", 
    335544576 : "Attempt to reopen an open cursor\n", 
    335544577 : "Attempt to reclose a closed cursor\n", 
    335544578 : "Column unknown\n", 
    335544579 : "Internal error\n", 
    335544580 : "Table unknown\n", 
    335544581 : "Procedure unknown\n", 
    335544582 : "Request unknown\n", 
    335544583 : "SQLDA missing or incorrect version, or incorrect number/type of variables\n", 
    335544584 : "Count of read-write columns does not equal count of values\n", 
    335544585 : "Invalid statement handle\n", 
    335544586 : "Function unknown\n", 
    335544587 : "Column is not a BLOB\n", 
    335544588 : "COLLATION @1 for CHARACTER SET @2 is not defined\n", 
    335544589 : "COLLATION @1 is not valid for specified CHARACTER SET\n", 
    335544590 : "Option specified more than once\n", 
    335544591 : "Unknown transaction option\n", 
    335544592 : "Invalid array reference\n", 
    335544593 : "Array declared with too many dimensions\n", 
    335544594 : "Illegal array dimension range\n", 
    335544595 : "Trigger unknown\n", 
    335544596 : "Subselect illegal in this context\n", 
    335544597 : "Cannot prepare a CREATE DATABASE/SCHEMA statement\n", 
    335544598 : "must specify column name for view select expression\n", 
    335544599 : "number of columns does not match select list\n", 
    335544600 : "Only simple column names permitted for VIEW WITH CHECK OPTION\n", 
    335544601 : "No WHERE clause for VIEW WITH CHECK OPTION\n", 
    335544602 : "Only one table allowed for VIEW WITH CHECK OPTION\n", 
    335544603 : "DISTINCT, GROUP or HAVING not permitted for VIEW WITH CHECK OPTION\n", 
    335544604 : "FOREIGN KEY column count does not match PRIMARY KEY\n", 
    335544605 : "No subqueries permitted for VIEW WITH CHECK OPTION\n", 
    335544606 : "expression evaluation not supported\n", 
    335544607 : "gen.c: node not supported\n", 
    335544608 : "Unexpected end of command\n", 
    335544609 : "INDEX @1\n", 
    335544610 : "EXCEPTION @1\n", 
    335544611 : "COLUMN @1\n", 
    335544612 : "Token unknown\n", 
    335544613 : "union not supported\n", 
    335544614 : "Unsupported DSQL construct\n", 
    335544615 : "column used with aggregate\n", 
    335544616 : "invalid column reference\n", 
    335544617 : "invalid ORDER BY clause\n", 
    335544618 : "Return mode by value not allowed for this data type\n", 
    335544619 : "External functions cannot have more than 10 parameters\n", 
    335544620 : "alias @1 conflicts with an alias in the same statement\n", 
    335544621 : "alias @1 conflicts with a procedure in the same statement\n", 
    335544622 : "alias @1 conflicts with a table in the same statement\n", 
    335544623 : "Illegal use of keyword VALUE\n", 
    335544624 : "segment count of 0 defined for index @1\n", 
    335544625 : "A node name is not permitted in a secondary, shadow, cache or log file name\n", 
    335544626 : "TABLE @1\n", 
    335544627 : "PROCEDURE @1\n", 
    335544628 : "cannot create index @1\n", 
    335544629 : "Write-ahead Log with shadowing configuration not allowed\n", 
    335544630 : "there are @1 dependencies\n", 
    335544631 : "too many keys defined for index @1\n", 
    335544632 : "Preceding file did not specify length, so @1 must include starting page number\n", 
    335544633 : "Shadow number must be a positive integer\n", 
    335544634 : "Token unknown - line @1, column @2\n", 
    335544635 : "there is no alias or table named @1 at this scope level\n", 
    335544636 : "there is no index @1 for table @2\n", 
    335544637 : "table @1 is not referenced in plan\n", 
    335544638 : "table @1 is referenced more than once in plan; use aliases to distinguish\n", 
    335544639 : "table @1 is referenced in the plan but not the from list\n", 
    335544640 : "Invalid use of CHARACTER SET or COLLATE\n", 
    335544641 : "Specified domain or source column @1 does not exist\n", 
    335544642 : "index @1 cannot be used in the specified plan\n", 
    335544643 : "the table @1 is referenced twice; use aliases to differentiate\n", 
    335544644 : "illegal operation when at beginning of stream\n", 
    335544645 : "the current position is on a crack\n", 
    335544646 : "database or file exists\n", 
    335544647 : "invalid comparison operator for find operation\n", 
    335544648 : "Connection lost to pipe server\n", 
    335544649 : "bad checksum\n", 
    335544650 : "wrong page type\n", 
    335544651 : "Cannot insert because the file is readonly or is on a read only medium.\n", 
    335544652 : "multiple rows in singleton select\n", 
    335544653 : "cannot attach to password database\n", 
    335544654 : "cannot start transaction for password database\n", 
    335544655 : "invalid direction for find operation\n", 
    335544656 : "variable @1 conflicts with parameter in same procedure\n", 
    335544657 : "Array/BLOB/DATE data types not allowed in arithmetic\n", 
    335544658 : "@1 is not a valid base table of the specified view\n", 
    335544659 : "table @1 is referenced twice in view; use an alias to distinguish\n", 
    335544660 : "view @1 has more than one base table; use aliases to distinguish\n", 
    335544661 : "cannot add index, index root page is full.\n", 
    335544662 : "BLOB SUB_TYPE @1 is not defined\n", 
    335544663 : "Too many concurrent executions of the same request\n", 
    335544664 : "duplicate specification of @1 - not supported\n", 
    335544665 : "violation of PRIMARY or UNIQUE KEY constraint \"@1\" on table \"@2\"\n", 
    335544666 : "server version too old to support all CREATE DATABASE options\n", 
    335544667 : "drop database completed with errors\n", 
    335544668 : "procedure @1 does not return any values\n", 
    335544669 : "count of column list and variable list do not match\n", 
    335544670 : "attempt to index BLOB column in index @1\n", 
    335544671 : "attempt to index array column in index @1\n", 
    335544672 : "too few key columns found for index @1 (incorrect column name?)\n", 
    335544673 : "cannot delete\n", 
    335544674 : "last column in a table cannot be deleted\n", 
    335544675 : "sort error\n", 
    335544676 : "sort error: not enough memory\n", 
    335544677 : "too many versions\n", 
    335544678 : "invalid key position\n", 
    335544679 : "segments not allowed in expression index @1\n", 
    335544680 : "sort error: corruption in data structure\n", 
    335544681 : "new record size of @1 bytes is too big\n", 
    335544682 : "Inappropriate self-reference of column\n", 
    335544683 : "request depth exceeded. (Recursive definition?)\n", 
    335544684 : "cannot access column @1 in view @2\n", 
    335544685 : "dbkey not available for multi-table views\n", 
    335544686 : "journal file wrong format\n", 
    335544687 : "intermediate journal file full\n", 
    335544688 : "The prepare statement identifies a prepare statement with an open cursor\n", 
    335544689 : "Firebird error\n", 
    335544690 : "Cache redefined\n", 
    335544691 : "Insufficient memory to allocate page buffer cache\n", 
    335544692 : "Log redefined\n", 
    335544693 : "Log size too small\n", 
    335544694 : "Log partition size too small\n", 
    335544695 : "Partitions not supported in series of log file specification\n", 
    335544696 : "Total length of a partitioned log must be specified\n", 
    335544697 : "Precision must be from 1 to 18\n", 
    335544698 : "Scale must be between zero and precision\n", 
    335544699 : "Short integer expected\n", 
    335544700 : "Long integer expected\n", 
    335544701 : "Unsigned short integer expected\n", 
    335544702 : "Invalid ESCAPE sequence\n", 
    335544703 : "service @1 does not have an associated executable\n", 
    335544704 : "Failed to locate host machine.\n", 
    335544705 : "Undefined service @1/@2.\n", 
    335544706 : "The specified name was not found in the hosts file or Domain Name Services.\n", 
    335544707 : "user does not have GRANT privileges on base table/view for operation\n", 
    335544708 : "Ambiguous column reference.\n", 
    335544709 : "Invalid aggregate reference\n", 
    335544710 : "navigational stream @1 references a view with more than one base table\n", 
    335544711 : "Attempt to execute an unprepared dynamic SQL statement.\n", 
    335544712 : "Positive value expected\n", 
    335544713 : "Incorrect values within SQLDA structure\n", 
    335544714 : "invalid blob id\n", 
    335544715 : "Operation not supported for EXTERNAL FILE table @1\n", 
    335544716 : "Service is currently busy: @1\n", 
    335544717 : "stack size insufficent to execute current request\n", 
    335544718 : "Invalid key for find operation\n", 
    335544719 : "Error initializing the network software.\n", 
    335544720 : "Unable to load required library @1.\n", 
    335544721 : "Unable to complete network request to host \"@1\".\n", 
    335544722 : "Failed to establish a connection.\n", 
    335544723 : "Error while listening for an incoming connection.\n", 
    335544724 : "Failed to establish a secondary connection for event processing.\n", 
    335544725 : "Error while listening for an incoming event connection request.\n", 
    335544726 : "Error reading data from the connection.\n", 
    335544727 : "Error writing data to the connection.\n", 
    335544728 : "Cannot deactivate index used by an integrity constraint\n", 
    335544729 : "Cannot deactivate index used by a PRIMARY/UNIQUE constraint\n", 
    335544730 : "Client/Server Express not supported in this release\n", 
    335544731 : "\n", 
    335544732 : "Access to databases on file servers is not supported.\n", 
    335544733 : "Error while trying to create file\n", 
    335544734 : "Error while trying to open file\n", 
    335544735 : "Error while trying to close file\n", 
    335544736 : "Error while trying to read from file\n", 
    335544737 : "Error while trying to write to file\n", 
    335544738 : "Error while trying to delete file\n", 
    335544739 : "Error while trying to access file\n", 
    335544740 : "A fatal exception occurred during the execution of a user defined function.\n", 
    335544741 : "connection lost to database\n", 
    335544742 : "User cannot write to RDB$USER_PRIVILEGES\n", 
    335544743 : "token size exceeds limit\n", 
    335544744 : "Maximum user count exceeded.  Contact your database administrator.\n", 
    335544745 : "Your login @1 is same as one of the SQL role name. Ask your database administrator to set up a valid Firebird login.\n", 
    335544746 : "\"REFERENCES table\" without \"(column)\" requires PRIMARY KEY on referenced table\n", 
    335544747 : "The username entered is too long.  Maximum length is 31 bytes.\n", 
    335544748 : "The password specified is too long.  Maximum length is 8 bytes.\n", 
    335544749 : "A username is required for this operation.\n", 
    335544750 : "A password is required for this operation\n", 
    335544751 : "The network protocol specified is invalid\n", 
    335544752 : "A duplicate user name was found in the security database\n", 
    335544753 : "The user name specified was not found in the security database\n", 
    335544754 : "An error occurred while attempting to add the user.\n", 
    335544755 : "An error occurred while attempting to modify the user record.\n", 
    335544756 : "An error occurred while attempting to delete the user record.\n", 
    335544757 : "An error occurred while updating the security database.\n", 
    335544758 : "sort record size of @1 bytes is too big\n", 
    335544759 : "can not define a not null column with NULL as default value\n", 
    335544760 : "invalid clause --- '@1'\n", 
    335544761 : "too many open handles to database\n", 
    335544762 : "size of optimizer block exceeded\n", 
    335544763 : "a string constant is delimited by double quotes\n", 
    335544764 : "DATE must be changed to TIMESTAMP\n", 
    335544765 : "attempted update on read-only database\n", 
    335544766 : "SQL dialect @1 is not supported in this database\n", 
    335544767 : "A fatal exception occurred during the execution of a blob filter.\n", 
    335544768 : "Access violation.  The code attempted to access a virtual address without privilege to do so.\n", 
    335544769 : "Datatype misalignment.  The attempted to read or write a value that was not stored on a memory boundary.\n", 
    335544770 : "Array bounds exceeded.  The code attempted to access an array element that is out of bounds.\n", 
    335544771 : "Float denormal operand.  One of the floating-point operands is too small to represent a standard float value.\n", 
    335544772 : "Floating-point divide by zero.  The code attempted to divide a floating-point value by zero.\n", 
    335544773 : "Floating-point inexact result.  The result of a floating-point operation cannot be represented as a deciaml fraction.\n", 
    335544774 : "Floating-point invalid operand.  An indeterminant error occurred during a floating-point operation.\n", 
    335544775 : "Floating-point overflow.  The exponent of a floating-point operation is greater than the magnitude allowed.\n", 
    335544776 : "Floating-point stack check.  The stack overflowed or underflowed as the result of a floating-point operation.\n", 
    335544777 : "Floating-point underflow.  The exponent of a floating-point operation is less than the magnitude allowed.\n", 
    335544778 : "Integer divide by zero.  The code attempted to divide an integer value by an integer divisor of zero.\n", 
    335544779 : "Integer overflow.  The result of an integer operation caused the most significant bit of the result to carry.\n", 
    335544780 : "An exception occurred that does not have a description.  Exception number @1.\n", 
    335544781 : "Stack overflow.  The resource requirements of the runtime stack have exceeded the memory available to it.\n", 
    335544782 : "Segmentation Fault. The code attempted to access memory without priviledges.\n", 
    335544783 : "Illegal Instruction. The Code attempted to perfrom an illegal operation.\n", 
    335544784 : "Bus Error. The Code caused a system bus error.\n", 
    335544785 : "Floating Point Error. The Code caused an Arithmetic Exception or a floating point exception.\n", 
    335544786 : "Cannot delete rows from external files.\n", 
    335544787 : "Cannot update rows in external files.\n", 
    335544788 : "Unable to perform operation.  You must be either SYSDBA or owner of the database\n", 
    335544789 : "Specified EXTRACT part does not exist in input datatype\n", 
    335544790 : "Service @1 requires SYSDBA permissions.  Reattach to the Service Manager using the SYSDBA account.\n", 
    335544791 : "The file @1 is currently in use by another process.  Try again later.\n", 
    335544792 : "Cannot attach to services manager\n", 
    335544793 : "Metadata update statement is not allowed by the current database SQL dialect @1\n", 
    335544794 : "operation was cancelled\n", 
    335544795 : "unexpected item in service parameter block, expected @1\n", 
    335544796 : "Client SQL dialect @1 does not support reference to @2 datatype\n", 
    335544797 : "user name and password are required while attaching to the services manager\n", 
    335544798 : "You created an indirect dependency on uncommitted metadata. You must roll back the current transaction.\n", 
    335544799 : "The service name was not specified.\n", 
    335544800 : "Too many Contexts of Relation/Procedure/Views. Maximum allowed is 255\n", 
    335544801 : "data type not supported for arithmetic\n", 
    335544802 : "Database dialect being changed from 3 to 1\n", 
    335544803 : "Database dialect not changed.\n", 
    335544804 : "Unable to create database @1\n", 
    335544805 : "Database dialect @1 is not a valid dialect.\n", 
    335544806 : "Valid database dialects are @1.\n", 
    335544807 : "SQL warning code = @1\n", 
    335544808 : "DATE data type is now called TIMESTAMP\n", 
    335544809 : "Function @1 is in @2, which is not in a permitted directory for external functions.\n", 
    335544810 : "value exceeds the range for valid dates\n", 
    335544811 : "passed client dialect @1 is not a valid dialect.\n", 
    335544812 : "Valid client dialects are @1.\n", 
    335544813 : "Unsupported field type specified in BETWEEN predicate.\n", 
    335544814 : "Services functionality will be supported in a later version  of the product\n", 
    335544815 : "GENERATOR @1\n", 
    335544816 : "UDF @1\n", 
    335544817 : "Invalid parameter to FIRST.  Only integers >= 0 are allowed.\n", 
    335544818 : "Invalid parameter to SKIP.  Only integers >= 0 are allowed.\n", 
    335544819 : "File exceeded maximum size of 2GB.  Add another database file or use a 64 bit I/O version of Firebird.\n", 
    335544820 : "Unable to find savepoint with name @1 in transaction context\n", 
    335544821 : "Invalid column position used in the @1 clause\n", 
    335544822 : "Cannot use an aggregate function in a WHERE clause, use HAVING instead\n", 
    335544823 : "Cannot use an aggregate function in a GROUP BY clause\n", 
    335544824 : "Invalid expression in the @1 (not contained in either an aggregate function or the GROUP BY clause)\n", 
    335544825 : "Invalid expression in the @1 (neither an aggregate function nor a part of the GROUP BY clause)\n", 
    335544826 : "Nested aggregate functions are not allowed\n", 
    335544827 : "Invalid argument in EXECUTE STATEMENT - cannot convert to string\n", 
    335544828 : "Wrong request type in EXECUTE STATEMENT '@1'\n", 
    335544829 : "Variable type (position @1) in EXECUTE STATEMENT '@2' INTO does not match returned column type\n", 
    335544830 : "Too many recursion levels of EXECUTE STATEMENT\n", 
    335544831 : "Access to @1 \"@2\" is denied by server administrator\n", 
    335544832 : "Cannot change difference file name while database is in backup mode\n", 
    335544833 : "Physical backup is not allowed while Write-Ahead Log is in use\n", 
    335544834 : "Cursor is not open\n", 
    335544835 : "Target shutdown mode is invalid for database \"@1\"\n", 
    335544836 : "Concatenation overflow. Resulting string cannot exceed 32K in length.\n", 
    335544837 : "Invalid offset parameter @1 to SUBSTRING. Only positive integers are allowed.\n", 
    335544838 : "Foreign key reference target does not exist\n", 
    335544839 : "Foreign key references are present for the record\n", 
    335544840 : "cannot update\n", 
    335544841 : "Cursor is already open\n", 
    335544842 : "@1\n", 
    335544843 : "Context variable @1 is not found in namespace @2\n", 
    335544844 : "Invalid namespace name @1 passed to @2\n", 
    335544845 : "Too many context variables\n", 
    335544846 : "Invalid argument passed to @1\n", 
    335544847 : "BLR syntax error. Identifier @1... is too long\n", 
    335544848 : "exception @1\n", 
    335544849 : "Malformed string\n", 
    335544850 : "Output parameter mismatch for procedure @1\n", 
    335544851 : "Unexpected end of command - line @1, column @2\n", 
    335544852 : "partner index segment no @1 has incompatible data type\n", 
    335544853 : "Invalid length parameter @1 to SUBSTRING. Negative integers are not allowed.\n", 
    335544854 : "CHARACTER SET @1 is not installed\n", 
    335544855 : "COLLATION @1 for CHARACTER SET @2 is not installed\n", 
    335544856 : "connection shutdown\n", 
    335544857 : "Maximum BLOB size exceeded\n", 
    335544858 : "Can't have relation with only computed fields or constraints\n", 
    335544859 : "Time precision exceeds allowed range (0-@1)\n", 
    335544860 : "Unsupported conversion to target type BLOB (subtype @1)\n", 
    335544861 : "Unsupported conversion to target type ARRAY\n", 
    335544862 : "Stream does not support record locking\n", 
    335544863 : "Cannot create foreign key constraint @1. Partner index does not exist or is inactive.\n", 
    335544864 : "Transactions count exceeded. Perform backup and restore to make database operable again\n", 
    335544865 : "Column has been unexpectedly deleted\n", 
    335544866 : "@1 cannot depend on @2\n", 
    335544867 : "Blob sub_types bigger than 1 (text) are for internal use only\n", 
    335544868 : "Procedure @1 is not selectable (it does not contain a SUSPEND statement)\n", 
    335544869 : "Datatype @1 is not supported for sorting operation\n", 
    335544870 : "COLLATION @1\n", 
    335544871 : "DOMAIN @1\n", 
    335544872 : "domain @1 is not defined\n", 
    335544873 : "Array data type can use up to @1 dimensions\n", 
    335544874 : "A multi database transaction cannot span more than @1 databases\n", 
    335544875 : "Bad debug info format\n", 
    335544876 : "Error while parsing procedure @1's BLR\n", 
    335544877 : "index key too big\n", 
    335544878 : "concurrent transaction number is @1\n", 
    335544879 : "validation error for variable @1, value \"@2\"\n", 
    335544880 : "validation error for @1, value \"@2\"\n", 
    335544881 : "Difference file name should be set explicitly for database on raw device\n", 
    335544882 : "Login name too long (@1 characters, maximum allowed @2)\n", 
    335544883 : "column @1 is not defined in procedure @2\n", 
    335544884 : "Invalid SIMILAR TO pattern\n", 
    335544885 : "Invalid TEB format\n", 
    335544886 : "Found more than one transaction isolation in TPB\n", 
    335544887 : "Table reservation lock type @1 requires table name before in TPB\n", 
    335544888 : "Found more than one @1 specification in TPB\n", 
    335544889 : "Option @1 requires READ COMMITTED isolation in TPB\n", 
    335544890 : "Option @1 is not valid if @2 was used previously in TPB\n", 
    335544891 : "Table name length missing after table reservation @1 in TPB\n", 
    335544892 : "Table name length @1 is too long after table reservation @2 in TPB\n", 
    335544893 : "Table name length @1 without table name after table reservation @2 in TPB\n", 
    335544894 : "Table name length @1 goes beyond the remaining TPB size after table reservation @2\n", 
    335544895 : "Table name length is zero after table reservation @1 in TPB\n", 
    335544896 : "Table or view @1 not defined in system tables after table reservation @2 in TPB\n", 
    335544897 : "Base table or view @1 for view @2 not defined in system tables after table reservation @3 in TPB\n", 
    335544898 : "Option length missing after option @1 in TPB\n", 
    335544899 : "Option length @1 without value after option @2 in TPB\n", 
    335544900 : "Option length @1 goes beyond the remaining TPB size after option @2\n", 
    335544901 : "Option length is zero after table reservation @1 in TPB\n", 
    335544902 : "Option length @1 exceeds the range for option @2 in TPB\n", 
    335544903 : "Option value @1 is invalid for the option @2 in TPB\n", 
    335544904 : "Preserving previous table reservation @1 for table @2, stronger than new @3 in TPB\n", 
    335544905 : "Table reservation @1 for table @2 already specified and is stronger than new @3 in TPB\n", 
    335544906 : "Table reservation reached maximum recursion of @1 when expanding views in TPB\n", 
    335544907 : "Table reservation in TPB cannot be applied to @1 because it's a virtual table\n", 
    335544908 : "Table reservation in TPB cannot be applied to @1 because it's a system table\n", 
    335544909 : "Table reservation @1 or @2 in TPB cannot be applied to @3 because it's a temporary table\n", 
    335544910 : "Cannot set the transaction in read only mode after a table reservation isc_tpb_lock_write in TPB\n", 
    335544911 : "Cannot take a table reservation isc_tpb_lock_write in TPB because the transaction is in read only mode\n", 
    335544912 : "value exceeds the range for a valid time\n", 
    335544913 : "value exceeds the range for valid timestamps\n", 
    335544914 : "string right truncation\n", 
    335544915 : "blob truncation when converting to a string: length limit exceeded\n", 
    335544916 : "numeric value is out of range\n", 
    335544917 : "Firebird shutdown is still in progress after the specified timeout\n", 
    335544918 : "Attachment handle is busy\n", 
    335544919 : "Bad written UDF detected: pointer returned in FREE_IT function was not allocated by ib_util_malloc\n", 
    335544920 : "External Data Source provider '@1' not found\n", 
    335544921 : "Execute statement error at @1 :@2Data source : @3\n", 
    335544922 : "Execute statement preprocess SQL error\n", 
    335544923 : "Statement expected\n", 
    335544924 : "Parameter name expected\n", 
    335544925 : "Unclosed comment found near '@1'\n", 
    335544926 : "Execute statement error at @1 :@2Statement : @3Data source : @4\n", 
    335544927 : "Input parameters mismatch\n", 
    335544928 : "Output parameters mismatch\n", 
    335544929 : "Input parameter '@1' have no value set\n", 
    335544930 : "BLR stream length @1 exceeds implementation limit @2\n", 
    335544931 : "Monitoring table space exhausted\n", 
    335544932 : "module name or entrypoint could not be found\n", 
    335544933 : "nothing to cancel\n", 
    335544934 : "ib_util library has not been loaded to deallocate memory returned by FREE_IT function\n", 
    335544935 : "Cannot have circular dependencies with computed fields\n", 
    335544936 : "Security database error\n", 
    335544937 : "Invalid data type in DATE/TIME/TIMESTAMP addition or subtraction in add_datettime()\n", 
    335544938 : "Only a TIME value can be added to a DATE value\n", 
    335544939 : "Only a DATE value can be added to a TIME value\n", 
    335544940 : "TIMESTAMP values can be subtracted only from another TIMESTAMP value\n", 
    335544941 : "Only one operand can be of type TIMESTAMP\n", 
    335544942 : "Only HOUR, MINUTE, SECOND and MILLISECOND can be extracted from TIME values\n", 
    335544943 : "HOUR, MINUTE, SECOND and MILLISECOND cannot be extracted from DATE values\n", 
    335544944 : "Invalid argument for EXTRACT() not being of DATE/TIME/TIMESTAMP type\n", 
    335544945 : "Arguments for @1 must be integral types or NUMERIC/DECIMAL without scale\n", 
    335544946 : "First argument for @1 must be integral type or floating point type\n", 
    335544947 : "Human readable UUID argument for @1 must be of string type\n", 
    335544948 : "Human readable UUID argument for @2 must be of exact length @1\n", 
    335544949 : "Human readable UUID argument for @3 must have \"-\" at position @2 instead of \"@1\"\n", 
    335544950 : "Human readable UUID argument for @3 must have hex digit at position @2 instead of \"@1\"\n", 
    335544951 : "Only HOUR, MINUTE, SECOND and MILLISECOND can be added to TIME values in @1\n", 
    335544952 : "Invalid data type in addition of part to DATE/TIME/TIMESTAMP in @1\n", 
    335544953 : "Invalid part @1 to be added to a DATE/TIME/TIMESTAMP value in @2\n", 
    335544954 : "Expected DATE/TIME/TIMESTAMP type in evlDateAdd() result\n", 
    335544955 : "Expected DATE/TIME/TIMESTAMP type as first and second argument to @1\n", 
    335544956 : "The result of TIME-<value> in @1 cannot be expressed in YEAR, MONTH, DAY or WEEK\n", 
    335544957 : "The result of TIME-TIMESTAMP or TIMESTAMP-TIME in @1 cannot be expressed in HOUR, MINUTE, SECOND or MILLISECOND\n", 
    335544958 : "The result of DATE-TIME or TIME-DATE in @1 cannot be expressed in HOUR, MINUTE, SECOND and MILLISECOND\n", 
    335544959 : "Invalid part @1 to express the difference between two DATE/TIME/TIMESTAMP values in @2\n", 
    335544960 : "Argument for @1 must be positive\n", 
    335544961 : "Base for @1 must be positive\n", 
    335544962 : "Argument #@1 for @2 must be zero or positive\n", 
    335544963 : "Argument #@1 for @2 must be positive\n", 
    335544964 : "Base for @1 cannot be zero if exponent is negative\n", 
    335544965 : "Base for @1 cannot be negative if exponent is not an integral value\n", 
    335544966 : "The numeric scale must be between -128 and 127 in @1\n", 
    335544967 : "Argument for @1 must be zero or positive\n", 
    335544968 : "Binary UUID argument for @1 must be of string type\n", 
    335544969 : "Binary UUID argument for @2 must use @1 bytes\n", 
    335544970 : "Missing required item @1 in service parameter block\n", 
    335544971 : "@1 server is shutdown\n", 
    335544972 : "Invalid connection string\n", 
    335544973 : "Unrecognized events block\n", 
    335544974 : "Could not start first worker thread - shutdown server\n", 
    335544975 : "Timeout occurred while waiting for a secondary connection for event processing\n", 
    335544976 : "Argument for @1 must be different than zero\n", 
    335544977 : "Argument for @1 must be in the range [-1, 1]\n", 
    335544978 : "Argument for @1 must be greater or equal than one\n", 
    335544979 : "Argument for @1 must be in the range ]-1, 1[\n", 
    335544980 : "Incorrect parameters provided to internal function @1\n", 
    335544981 : "Floating point overflow in built-in function @1\n", 
    335544982 : "Floating point overflow in result from UDF @1\n", 
    335544983 : "Invalid floating point value returned by UDF @1\n", 
    335544984 : "Database is probably already opened by another engine instance in another Windows session\n", 
    335544985 : "No free space found in temporary directories\n", 
    335544986 : "Explicit transaction control is not allowed\n", 
    335544987 : "Use of TRUSTED switches in spb_command_line is prohibited\n", 
    335545017 : "Asynchronous call is already running for this attachment\n", 
    335740929 : "data base file name (@1) already given\n", 
    335740930 : "invalid switch @1\n", 
    335740932 : "incompatible switch combination\n", 
    335740933 : "replay log pathname required\n", 
    335740934 : "number of page buffers for cache required\n", 
    335740935 : "numeric value required\n", 
    335740936 : "positive numeric value required\n", 
    335740937 : "number of transactions per sweep required\n", 
    335740940 : "\"full\" or \"reserve\" required\n", 
    335740941 : "user name required\n", 
    335740942 : "password required\n", 
    335740943 : "subsystem name\n", 
    335740944 : "\"wal\" required\n", 
    335740945 : "number of seconds required\n", 
    335740946 : "numeric value between 0 and 32767 inclusive required\n", 
    335740947 : "must specify type of shutdown\n", 
    335740948 : "please retry, specifying an option\n", 
    335740951 : "please retry, giving a database name\n", 
    335740991 : "internal block exceeds maximum size\n", 
    335740992 : "corrupt pool\n", 
    335740993 : "virtual memory exhausted\n", 
    335740994 : "bad pool id\n", 
    335740995 : "Transaction state @1 not in valid range.\n", 
    335741012 : "unexpected end of input\n", 
    335741018 : "failed to reconnect to a transaction in database @1\n", 
    335741036 : "Transaction description item unknown\n", 
    335741038 : "\"read_only\" or \"read_write\" required\n", 
    335741042 : "positive or zero numeric value required\n", 
    336003074 : "Cannot SELECT RDB$DB_KEY from a stored procedure.\n", 
    336003075 : "Precision 10 to 18 changed from DOUBLE PRECISION in SQL dialect 1 to 64-bit scaled integer in SQL dialect 3\n", 
    336003076 : "Use of @1 expression that returns different results in dialect 1 and dialect 3\n", 
    336003077 : "Database SQL dialect @1 does not support reference to @2 datatype\n", 
    336003079 : "DB dialect @1 and client dialect @2 conflict with respect to numeric precision @3.\n", 
    336003080 : "WARNING: Numeric literal @1 is interpreted as a floating-point\n", 
    336003081 : "value in SQL dialect 1, but as an exact numeric value in SQL dialect 3.\n", 
    336003082 : "WARNING: NUMERIC and DECIMAL fields with precision 10 or greater are stored\n", 
    336003083 : "as approximate floating-point values in SQL dialect 1, but as 64-bit\n", 
    336003084 : "integers in SQL dialect 3.\n", 
    336003085 : "Ambiguous field name between @1 and @2\n", 
    336003086 : "External function should have return position between 1 and @1\n", 
    336003087 : "Label @1 @2 in the current scope\n", 
    336003088 : "Datatypes @1are not comparable in expression @2\n", 
    336003089 : "Empty cursor name is not allowed\n", 
    336003090 : "Statement already has a cursor @1 assigned\n", 
    336003091 : "Cursor @1 is not found in the current context\n", 
    336003092 : "Cursor @1 already exists in the current context\n", 
    336003093 : "Relation @1 is ambiguous in cursor @2\n", 
    336003094 : "Relation @1 is not found in cursor @2\n", 
    336003095 : "Cursor is not open\n", 
    336003096 : "Data type @1 is not supported for EXTERNAL TABLES. Relation '@2', field '@3'\n", 
    336003097 : "Feature not supported on ODS version older than @1.@2\n", 
    336003098 : "Primary key required on table @1\n", 
    336003099 : "UPDATE OR INSERT field list does not match primary key of table @1\n", 
    336003100 : "UPDATE OR INSERT field list does not match MATCHING clause\n", 
    336003101 : "UPDATE OR INSERT without MATCHING could not be used with views based on more than one table\n", 
    336003102 : "Incompatible trigger type\n", 
    336003103 : "Database trigger type can't be changed\n", 
    336068740 : "Table @1 already exists\n", 
    336068784 : "column @1 does not exist in table/view @2\n", 
    336068796 : "SQL role @1 does not exist\n", 
    336068797 : "user @1 has no grant admin option on SQL role @2\n", 
    336068798 : "user @1 is not a member of SQL role @2\n", 
    336068799 : "@1 is not the owner of SQL role @2\n", 
    336068800 : "@1 is a SQL role and not a user\n", 
    336068801 : "user name @1 could not be used for SQL role\n", 
    336068802 : "SQL role @1 already exists\n", 
    336068803 : "keyword @1 can not be used as a SQL role name\n", 
    336068804 : "SQL roles are not supported in on older versions of the database.  A backup and restore of the database is required.\n", 
    336068812 : "Cannot rename domain @1 to @2.  A domain with that name already exists.\n", 
    336068813 : "Cannot rename column @1 to @2.  A column with that name already exists in table @3.\n", 
    336068814 : "Column @1 from table @2 is referenced in @3\n", 
    336068815 : "Cannot change datatype for column @1.  Changing datatype is not supported for BLOB or ARRAY columns.\n", 
    336068816 : "New size specified for column @1 must be at least @2 characters.\n", 
    336068817 : "Cannot change datatype for @1.  Conversion from base type @2 to @3 is not supported.\n", 
    336068818 : "Cannot change datatype for column @1 from a character type to a non-character type.\n", 
    336068820 : "Zero length identifiers are not allowed\n", 
    336068829 : "Maximum number of collations per character set exceeded\n", 
    336068830 : "Invalid collation attributes\n", 
    336068840 : "@1 cannot reference @2\n", 
    336068852 : "New scale specified for column @1 must be at most @2.\n", 
    336068853 : "New precision specified for column @1 must be at least @2.\n", 
    336068855 : "Warning: @1 on @2 is not granted to @3.\n", 
    336068856 : "Feature '@1' is not supported in ODS @2.@3\n", 
    336068857 : "Cannot add or remove COMPUTED from column @1\n", 
    336068858 : "Password should not be empty string\n", 
    336068859 : "Index @1 already exists\n", 
    336330753 : "found unknown switch\n", 
    336330754 : "page size parameter missing\n", 
    336330755 : "Page size specified (@1) greater than limit (16384 bytes)\n", 
    336330756 : "redirect location for output is not specified\n", 
    336330757 : "conflicting switches for backup/restore\n", 
    336330758 : "device type @1 not known\n", 
    336330759 : "protection is not there yet\n", 
    336330760 : "page size is allowed only on restore or create\n", 
    336330761 : "multiple sources or destinations specified\n", 
    336330762 : "requires both input and output filenames\n", 
    336330763 : "input and output have the same name.  Disallowed.\n", 
    336330764 : "expected page size, encountered \"@1\"\n", 
    336330765 : "REPLACE specified, but the first file @1 is a database\n", 
    336330766 : "database @1 already exists.  To replace it, use the -REP switch\n", 
    336330767 : "device type not specified\n", 
    336330772 : "gds_$blob_info failed\n", 
    336330773 : "do not understand BLOB INFO item @1\n", 
    336330774 : "gds_$get_segment failed\n", 
    336330775 : "gds_$close_blob failed\n", 
    336330776 : "gds_$open_blob failed\n", 
    336330777 : "Failed in put_blr_gen_id\n", 
    336330778 : "data type @1 not understood\n", 
    336330779 : "gds_$compile_request failed\n", 
    336330780 : "gds_$start_request failed\n", 
    336330781 : "gds_$receive failed\n", 
    336330782 : "gds_$release_request failed\n", 
    336330783 : "gds_$database_info failed\n", 
    336330784 : "Expected database description record\n", 
    336330785 : "failed to create database @1\n", 
    336330786 : "RESTORE: decompression length error\n", 
    336330787 : "cannot find table @1\n", 
    336330788 : "Cannot find column for BLOB\n", 
    336330789 : "gds_$create_blob failed\n", 
    336330790 : "gds_$put_segment failed\n", 
    336330791 : "expected record length\n", 
    336330792 : "wrong length record, expected @1 encountered @2\n", 
    336330793 : "expected data attribute\n", 
    336330794 : "Failed in store_blr_gen_id\n", 
    336330795 : "do not recognize record type @1\n", 
    336330796 : "Expected backup version 1..9.  Found @1\n", 
    336330797 : "expected backup description record\n", 
    336330798 : "string truncated\n", 
    336330799 : "warning -- record could not be restored\n", 
    336330800 : "gds_$send failed\n", 
    336330801 : "no table name for data\n", 
    336330802 : "unexpected end of file on backup file\n", 
    336330803 : "database format @1 is too old to restore to\n", 
    336330804 : "array dimension for column @1 is invalid\n", 
    336330807 : "Expected XDR record length\n", 
    336330817 : "cannot open backup file @1\n", 
    336330818 : "cannot open status and error output file @1\n", 
    336330934 : "blocking factor parameter missing\n", 
    336330935 : "expected blocking factor, encountered \"@1\"\n", 
    336330936 : "a blocking factor may not be used in conjunction with device CT\n", 
    336330940 : "user name parameter missing\n", 
    336330941 : "password parameter missing\n", 
    336330952 : " missing parameter for the number of bytes to be skipped\n", 
    336330953 : "expected number of bytes to be skipped, encountered \"@1\"\n", 
    336330965 : "character set\n", 
    336330967 : "collation\n", 
    336330972 : "Unexpected I/O error while reading from backup file\n", 
    336330973 : "Unexpected I/O error while writing to backup file\n", 
    336330985 : "could not drop database @1 (database might be in use)\n", 
    336330990 : "System memory exhausted\n", 
    336331002 : "SQL role\n", 
    336331005 : "SQL role parameter missing\n", 
    336331010 : "page buffers parameter missing\n", 
    336331011 : "expected page buffers, encountered \"@1\"\n", 
    336331012 : "page buffers is allowed only on restore or create\n", 
    336331014 : "size specification either missing or incorrect for file @1\n", 
    336331015 : "file @1 out of sequence\n", 
    336331016 : "can't join -- one of the files missing\n", 
    336331017 : " standard input is not supported when using join operation\n", 
    336331018 : "standard output is not supported when using split operation\n", 
    336331019 : "backup file @1 might be corrupt\n", 
    336331020 : "database file specification missing\n", 
    336331021 : "can't write a header record to file @1\n", 
    336331022 : "free disk space exhausted\n", 
    336331023 : "file size given (@1) is less than minimum allowed (@2)\n", 
    336331025 : "service name parameter missing\n", 
    336331026 : "Cannot restore over current database, must be SYSDBA or owner of the existing database.\n", 
    336331031 : "\"read_only\" or \"read_write\" required\n", 
    336331033 : "just data ignore all constraints etc.\n", 
    336331034 : "restoring data only ignoring foreign key, unique, not null & other constraints\n", 
    336331093 : "Invalid metadata detected. Use -FIX_FSS_METADATA option.\n", 
    336331094 : "Invalid data detected. Use -FIX_FSS_DATA option.\n", 
    336397205 : "ODS versions before ODS@1 are not supported\n", 
    336397206 : "Table @1 does not exist\n", 
    336397207 : "View @1 does not exist\n", 
    336397208 : "At line @1, column @2\n", 
    336397209 : "At unknown line and column\n", 
    336397210 : "Column @1 cannot be repeated in @2 statement\n", 
    336397211 : "Too many values (more than @1) in member list to match against\n", 
    336397212 : "Array and BLOB data types not allowed in computed field\n", 
    336397213 : "Implicit domain name @1 not allowed in user created domain\n", 
    336397214 : "scalar operator used on field @1 which is not an array\n", 
    336397215 : "cannot sort on more than 255 items\n", 
    336397216 : "cannot group on more than 255 items\n", 
    336397217 : "Cannot include the same field (@1.@2) twice in the ORDER BY clause with conflicting sorting options\n", 
    336397218 : "column list from derived table @1 has more columns than the number of items in its SELECT statement\n", 
    336397219 : "column list from derived table @1 has less columns than the number of items in its SELECT statement\n", 
    336397220 : "no column name specified for column number @1 in derived table @2\n", 
    336397221 : "column @1 was specified multiple times for derived table @2\n", 
    336397222 : "Internal dsql error: alias type expected by pass1_expand_select_node\n", 
    336397223 : "Internal dsql error: alias type expected by pass1_field\n", 
    336397224 : "Internal dsql error: column position out of range in pass1_union_auto_cast\n", 
    336397225 : "Recursive CTE member (@1) can refer itself only in FROM clause\n", 
    336397226 : "CTE '@1' has cyclic dependencies\n", 
    336397227 : "Recursive member of CTE can't be member of an outer join\n", 
    336397228 : "Recursive member of CTE can't reference itself more than once\n", 
    336397229 : "Recursive CTE (@1) must be an UNION\n", 
    336397230 : "CTE '@1' defined non-recursive member after recursive\n", 
    336397231 : "Recursive member of CTE '@1' has @2 clause\n", 
    336397232 : "Recursive members of CTE (@1) must be linked with another members via UNION ALL\n", 
    336397233 : "Non-recursive member is missing in CTE '@1'\n", 
    336397234 : "WITH clause can't be nested\n", 
    336397235 : "column @1 appears more than once in USING clause\n", 
    336397236 : "feature is not supported in dialect @1\n", 
    336397237 : "CTE \"@1\" is not used in query\n", 
    336397238 : "column @1 appears more than once in ALTER VIEW\n", 
    336397239 : "@1 is not supported inside IN AUTONOMOUS TRANSACTION block\n", 
    336397240 : "Unknown node type @1 in dsql/GEN_expr\n", 
    336397241 : "Argument for @1 in dialect 1 must be string or numeric\n", 
    336397242 : "Argument for @1 in dialect 3 must be numeric\n", 
    336397243 : "Strings cannot be added to or subtracted from DATE or TIME types\n", 
    336397244 : "Invalid data type for subtraction involving DATE, TIME or TIMESTAMP types\n", 
    336397245 : "Adding two DATE values or two TIME values is not allowed\n", 
    336397246 : "DATE value cannot be subtracted from the provided data type\n", 
    336397247 : "Strings cannot be added or subtracted in dialect 3\n", 
    336397248 : "Invalid data type for addition or subtraction in dialect 3\n", 
    336397249 : "Invalid data type for multiplication in dialect 1\n", 
    336397250 : "Strings cannot be multiplied in dialect 3\n", 
    336397251 : "Invalid data type for multiplication in dialect 3\n", 
    336397252 : "Division in dialect 1 must be between numeric data types\n", 
    336397253 : "Strings cannot be divided in dialect 3\n", 
    336397254 : "Invalid data type for division in dialect 3\n", 
    336397255 : "Strings cannot be negated (applied the minus operator) in dialect 3\n", 
    336397256 : "Invalid data type for negation (minus operator)\n", 
    336397257 : "Cannot have more than 255 items in DISTINCT list\n", 
    336723983 : "unable to open database\n", 
    336723984 : "error in switch specifications\n", 
    336723985 : "no operation specified\n", 
    336723986 : "no user name specified\n", 
    336723987 : "add record error\n", 
    336723988 : "modify record error\n", 
    336723989 : "find/modify record error\n", 
    336723990 : "record not found for user: @1\n", 
    336723991 : "delete record error\n", 
    336723992 : "find/delete record error\n", 
    336723996 : "find/display record error\n", 
    336723997 : "invalid parameter, no switch defined\n", 
    336723998 : "operation already specified\n", 
    336723999 : "password already specified\n", 
    336724000 : "uid already specified\n", 
    336724001 : "gid already specified\n", 
    336724002 : "project already specified\n", 
    336724003 : "organization already specified\n", 
    336724004 : "first name already specified\n", 
    336724005 : "middle name already specified\n", 
    336724006 : "last name already specified\n", 
    336724008 : "invalid switch specified\n", 
    336724009 : "ambiguous switch specified\n", 
    336724010 : "no operation specified for parameters\n", 
    336724011 : "no parameters allowed for this operation\n", 
    336724012 : "incompatible switches specified\n", 
    336724044 : "Invalid user name (maximum 31 bytes allowed)\n", 
    336724045 : "Warning - maximum 8 significant bytes of password used\n", 
    336724046 : "database already specified\n", 
    336724047 : "database administrator name already specified\n", 
    336724048 : "database administrator password already specified\n", 
    336724049 : "SQL role name already specified\n", 
    336789504 : "The license file does not exist or could not be opened for read\n", 
    336789523 : "operation already specified\n", 
    336789524 : "no operation specified\n", 
    336789525 : "invalid switch\n", 
    336789526 : "invalid switch combination\n", 
    336789527 : "illegal operation/switch combination\n", 
    336789528 : "ambiguous switch\n", 
    336789529 : "invalid parameter, no switch specified\n", 
    336789530 : "switch does not take any parameter\n", 
    336789531 : "switch requires a parameter\n", 
    336789532 : "syntax error in command line\n", 
    336789534 : "The certificate was not added.  A duplicate ID exists in the license file.\n", 
    336789535 : "The certificate was not added.  Invalid certificate ID / Key combination.\n", 
    336789536 : "The certificate was not removed.  The key does not exist or corresponds to a temporary evaluation license.\n", 
    336789537 : "An error occurred updating the license file.  Operation cancelled.\n", 
    336789538 : "The certificate could not be validated based on the information given.  Please recheck the ID and key information.\n", 
    336789539 : "Operation failed.  An unknown error occurred.\n", 
    336789540 : "Add license operation failed, KEY: @1 ID: @2\n", 
    336789541 : "Remove license operation failed, KEY: @1\n", 
    336789563 : "The evaluation license has already been used on this server.  You need to purchase a non-evaluation license.\n", 
    336920577 : "found unknown switch\n", 
    336920578 : "please retry, giving a database name\n", 
    336920579 : "Wrong ODS version, expected @1, encountered @2\n", 
    336920580 : "Unexpected end of database file.\n", 
    336920605 : "Can't open database file @1\n", 
    336920606 : "Can't read a database page\n", 
    336920607 : "System memory exhausted\n", 
    336986113 : "Wrong value for access mode\n", 
    336986114 : "Wrong value for write mode\n", 
    336986115 : "Wrong value for reserve space\n", 
    336986116 : "Unknown tag (@1) in info_svr_db_info block after isc_svc_query()\n", 
    336986117 : "Unknown tag (@1) in isc_svc_query() results\n", 
    336986118 : "Unknown switch \"@1\"\n", 
    336986159 : "Wrong value for shutdown mode\n", 
    336986160 : "could not open file @1\n", 
    336986161 : "could not read file @1\n", 
    336986162 : "empty file @1\n", 
    336986164 : "Invalid or missing parameter for switch @1\n", 
    337051649 : "Switches trusted_svc and trusted_role are not supported from command line\n", 
}

type wireProtocol struct {
    buf []byte
    buffer_len int
    bufCount int

    conn net.Conn
    dbHandle int32
    addr string
}

func newWireProtocol (addr string) (*wireProtocol, error) {
    p := new(wireProtocol)
    p.buffer_len = 1024
    var err error
    p.buf = make([]byte, p.buffer_len)

    p.addr = addr
    p.conn, err = net.Dial("tcp", p.addr)
    if err != nil {
        return nil, err
    }

    return p, err
}

func (p *wireProtocol) packInt(i int32) {
    // pack big endian int32
    p.buf[p.bufCount+0] = byte(i >> 24 & 0xFF)
    p.buf[p.bufCount+1] = byte(i >> 16 & 0xFF)
    p.buf[p.bufCount+2] = byte(i >> 8 & 0xFF)
    p.buf[p.bufCount+3] = byte(i & 0xFF)
    p.bufCount += 4
}

func (p *wireProtocol) packBytes(b []byte) {
    for _, b := range xdrBytes(b) {
        p.buf[p.bufCount] = b
        p.bufCount++
    }
}

func (p *wireProtocol) packString(s string) {
    for _, b := range xdrString(s) {
        p.buf[p.bufCount] = b
        p.bufCount++
    }
}

func (p *wireProtocol) appendBytes(bs [] byte) {
    for _, b := range bs {
        p.buf[p.bufCount] = b
        p.bufCount++
    }
}

func (p *wireProtocol) uid() []byte {
    user := os.Getenv("USER")
    if user == "" {
        user = os.Getenv("USERNAME")
    }
    hostname, _ := os.Hostname()

    userBytes := bytes.NewBufferString(user).Bytes()
    hostnameBytes := bytes.NewBufferString(hostname).Bytes()
    return bytes.Join([][]byte{
        []byte{1, byte(len(userBytes))}, userBytes,
        []byte{4, byte(len(hostnameBytes))}, hostnameBytes,
        []byte{6, 4},
    }, nil)
}

func (p *wireProtocol) sendPackets() (n int, err error) {
    n, err = p.conn.Write(p.buf)
    return
}

func (p *wireProtocol) recvPackets(n int) ([]byte, error) {
    buf := make([]byte, n)
    _, err := p.conn.Read(buf)
    return buf, err
}

func (p *wireProtocol) recvPacketsAlignment(n int) ([]byte, error) {
    padding := n % 4
    if padding > 0 {
        padding = 4 - padding
    }
    buf, err := p.recvPackets(n + padding)
    return buf[0:n], err
}

func (p *wireProtocol) _parse_status_vector() (*list.List, int, string, error) {
    sql_code := 0
    gds_code := 0
    gds_codes := list.New()
    num_arg := 0
    message := ""

    b, err := p.recvPackets(4)
    n := bytes_to_bint32(b)
    for ;n != isc_arg_end; {
        switch {
        case n == isc_arg_gds:
            b, err = p.recvPackets(4)
            gds_code := int(bytes_to_bint32(b))
            if gds_code != 0 {
                gds_codes.PushBack(gds_code)
                message += errmsgs[gds_code]
                num_arg = 0
            }
        case n == isc_arg_number:
            b, err = p.recvPackets(4)
            num := int(bytes_to_bint32(b))
            if gds_code == 335544436 {
                sql_code = num
            }
            num_arg += 1
            message = strings.Replace(message, "@" + string(num_arg), string(num), 1)
        case n == isc_arg_string || n == isc_arg_interpreted:
            b, err = p.recvPackets(4)
            nbytes := int(bytes_to_bint32(b))
            b, err = p.recvPacketsAlignment(nbytes)
            s := bytes_to_str(b)
            num_arg += 1
            message = strings.Replace(message, "@" + string(num_arg), s, 1)
        }
        b, err = p.recvPackets(4)
        n = bytes_to_bint32(b)
    }

    return gds_codes, sql_code, message, err
}


func (p *wireProtocol) _parse_op_response() (int32, int32, []byte, error) {
    b, err := p.recvPackets(16)
    h := bytes_to_bint32(b[0:4])           // Object handle
    oid := bytes_to_bint32(b[4:12])                       // Object ID
    buf_len := int(bytes_to_bint32(b[12:]))     // buffer length
    buf, err := p.recvPacketsAlignment(buf_len)

    _, sql_code, message, err := p._parse_status_vector()
    if sql_code != 0 || message != "" {
        err = errors.New(message)
    }

    return h, oid, buf, err
}

func (p *wireProtocol) opConnect(dbName string) {
    p.packInt(op_connect)
    p.packInt(op_attach)
    p.packInt(2)   // CONNECT_VERSION2
    p.packInt(1)   // Arch type (Generic = 1)
    p.packString(dbName)
    p.packInt(1)   // Protocol version understood count.
    p.packBytes(p.uid())
    p.packInt(10)  // PROTOCOL_VERSION10
    p.packInt(1)   // Arch type (Generic = 1)
    p.packInt(2)   // Min type
    p.packInt(3)   // Max type
    p.packInt(2)   // Preference weight
    p.sendPackets()
}


func (p *wireProtocol) opCreate(dbName string, user string, passwd string) {
    var page_size int32
    page_size = 4096

    encode := bytes.NewBufferString("UTF8").Bytes()
    userBytes := bytes.NewBufferString(user).Bytes()
    passwdBytes := bytes.NewBufferString(passwd).Bytes()
    dpb := bytes.Join([][]byte{
        []byte{1},
        []byte{68, byte(len(encode))}, encode,
        []byte{48, byte(len(encode))}, encode,
        []byte{28, byte(len(userBytes))}, userBytes,
        []byte{29, byte(len(passwdBytes))}, passwdBytes,
        []byte{63, 4}, int32_to_bytes(3),
        []byte{24, 4}, bint32_to_bytes(1),
        []byte{54, 4}, bint32_to_bytes(1),
        []byte{4, 4}, int32_to_bytes(page_size),
    }, nil)

    p.packInt(op_create)
    p.packInt(0)                       // Database Object ID
    p.packString(dbName)
    p.packBytes(dpb)
    p.sendPackets()
}

func (p *wireProtocol) opAccept() {
    b, _ := p.recvPackets(4)
    for {
        if bytes_to_bint32(b) == op_dummy {
            b, _ = p.recvPackets(4)
        }
    }

    // assert bytes_to_bint32(b) == op_accept
    b, _ = p.recvPackets(12)
    // assert up.unpack_int() == 10
    // assert  up.unpack_int() == 1
    // assert up.unpack_int() == 3
}

func (p *wireProtocol) opAttach(dbName string, user string, passwd string) {
    encode := bytes.NewBufferString("UTF8").Bytes()
    userBytes := bytes.NewBufferString(user).Bytes()
    passwdBytes := bytes.NewBufferString(passwd).Bytes()

    dbp := bytes.Join([][]byte{
        []byte{1},
        []byte{48, byte(len(encode))}, encode,
        []byte{28, byte(len(userBytes))}, userBytes,
        []byte{29, byte(len(passwdBytes))}, passwdBytes,
    }, nil)
    p.packInt(op_attach)
    p.packInt(0)                       // Database Object ID
    p.packString(dbName)
    p.packBytes(dbp)
    p.sendPackets()
}

func (p *wireProtocol) opDropDatabase() {
    p.packInt(op_drop_database)
    p.packInt(p.dbHandle)
    p.sendPackets()
}


func (p *wireProtocol) opTransaction(tpb []byte) {
    p.packInt(op_transaction)
    p.packInt(p.dbHandle)
    p.packBytes(tpb)
    p.sendPackets()
}

func (p *wireProtocol) opCommit(transHandle int32) {
    p.packInt(op_commit)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opCommitRetaining(transHandle int32) {
    p.packInt(op_commit_retaining)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opRollback(transHandle int32) {
    p.packInt(op_rollback)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opRollbackRetaining(transHandle int32) {
    p.packInt(op_rollback_retaining)
    p.packInt(transHandle)
    p.sendPackets()
}

func (p *wireProtocol) opAallocateStatement() {
    p.packInt(op_allocate_statement)
    p.packInt(p.dbHandle)
    p.sendPackets()
}

func (p *wireProtocol) opInfoTransaction(transHandle int32 , b []byte) {
    p.packInt(op_info_transaction)
    p.packInt(transHandle)
    p.packInt(0)
    p.packBytes(b)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opInfoDatabase(bs []byte) {
    p.packInt(op_info_database)
    p.packInt(p.dbHandle)
    p.packInt(0)
    p.packBytes(bs)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opFreeStatement(stmtHandle int32, mode int32) {
    p.packInt(op_free_statement)
    p.packInt(stmtHandle)
    p.packInt(mode)
    p.sendPackets()
}

func (p *wireProtocol) opPrepareStatement(stmtHandle int32, transHandle int32, query string) {

    descItems := []byte{
        isc_info_sql_stmt_type,
        isc_info_sql_num_variables,
        isc_info_sql_select,
        isc_info_sql_describe_vars,
        isc_info_sql_sqlda_seq,
        isc_info_sql_type,
        isc_info_sql_sub_type,
        isc_info_sql_scale,
        isc_info_sql_length,
        isc_info_sql_null_ind,
        isc_info_sql_field,
        isc_info_sql_relation,
        isc_info_sql_owner,
        isc_info_sql_alias,
        isc_info_sql_describe_end,
    }

    p.packInt(op_prepare_statement)
    p.packInt(transHandle)
    p.packInt(stmtHandle)
    p.packInt(3)                        // dialect = 3
    p.packString(query)
    p.packBytes(descItems)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opInfoSql(stmtHandle int32, vars []byte) {
    p.packInt(op_info_sql)
    p.packInt(stmtHandle)
    p.packInt(0)
    p.packBytes(vars)
    p.packInt(int32(p.buffer_len))
    p.sendPackets()
}

func (p *wireProtocol) opExecute(stmtHandle int32, transHandle int32, params []interface{}) {
    p.packInt(op_execute)
    p.packInt(stmtHandle)
    p.packInt(transHandle)

    if len(params) == 0 {
        p.packInt(0)        // packBytes([])
        p.packInt(0)
        p.packInt(0)
        p.sendPackets()
    } else {
        blr, values := paramsToBlr(params)
        p.packBytes(blr)
        p.packInt(0)
        p.packInt(1)
        p.appendBytes(values)
        p.sendPackets()
    }
}

func (p *wireProtocol) opExecute2(stmtHandle int32, transHandle int32, params []interface{}, outputBlr []byte) {
    p.packInt(op_execute2)
    p.packInt(stmtHandle)
    p.packInt(transHandle)

    if len(params) == 0 {
        p.packInt(0)        // packBytes([])
        p.packInt(0)
        p.packInt(0)
    } else {
        blr, values := paramsToBlr(params)
        p.packBytes(blr)
        p.packInt(0)
        p.packInt(1)
        p.appendBytes(values)
    }

    p.packBytes(outputBlr)
    p.packInt(0)
    p.sendPackets()
}

func (p *wireProtocol)  opFetch(stmtHandle int32, blr []byte) {
    p.packInt(op_fetch)
    p.packInt(stmtHandle)
    p.packBytes(blr)
    p.packInt(0)
    p.packInt(400)
    p.sendPackets()
}

func (p *wireProtocol) opFetchResponse(stmtHandle int32, xsqlda []xSQLVAR) (*list.List, error) {
    b, err := p.recvPackets(4)
    for {
        if bytes_to_bint32(b) == op_dummy {
            b, err = p.recvPackets(4)
        }
    }

    if bytes_to_bint32(b) == op_response {
        p._parse_op_response()      // error occured
        return nil, errors.New("opFetchResponse:Internal Error")
    }
    if bytes_to_bint32(b) != op_fetch_response {
        return nil, errors.New("opFetchResponse:Internal Error")
    }
    b, err = p.recvPackets(8)
    status := bytes_to_bint32(b[:4])
    count := int(bytes_to_bint32(b[4:8]))
    rows := list.New()
    for ; count > 0; {
        r := list.New()
        for _, x := range xsqlda {
            var ln int
            if x.ioLength() < 0 {
                b, err = p.recvPackets(4)
                ln = int(bytes_to_bint32(b))
            } else {
                ln = x.ioLength()
            }
            raw_value, _ := p.recvPacketsAlignment(ln)
            b, err = p.recvPackets(4)
            if bytes_to_bint32(b) == 0 { // Not NULL
                r.PushBack(x.value(raw_value))
            }
        }
        rows.PushBack(r)

        b, err = p.recvPackets(12)
        // op := int(bytes_to_bint32(b[:4]))
        status = bytes_to_bint32(b[4:8])
        count = int(bytes_to_bint32(b[8:]))
    }
    if status == 100 {
        err = errors.New("Error: op_fetch_response")
    }
        
    return rows, err
}

func (p *wireProtocol) opDetach() {
    p.packInt(op_detach)
    p.packInt(p.dbHandle)
    p.sendPackets()
}

func (p *wireProtocol)  opOpenBlob(blobId int32, transHandle int32) {
    p.packInt(op_open_blob)
    p.packInt(transHandle)
    p.packInt(blobId)
    p.sendPackets()
}

func (p *wireProtocol)  opCreateBlob2(transHandle int32) {
    p.packInt(op_create_blob2)
    p.packInt(0)
    p.packInt(transHandle)
    p.packInt(0)
    p.packInt(0)
    p.sendPackets()
}

func (p *wireProtocol) opGetSegment(blobHandle int32) {
    p.packInt(op_get_segment)
    p.packInt(blobHandle)
    p.packInt(int32(p.buffer_len))
    p.packInt(0)
    p.sendPackets()
}

func (p *wireProtocol) opBatchSegments(blobHandle int32, seg_data []byte) {
    ln := len(seg_data)
    p.packInt(op_batch_segments)
    p.packInt(blobHandle)
    p.packInt(int32(ln + 2))
    p.packInt(int32(ln + 2))
    pad_length := ((4-(ln+2)) & 3)
    padding := make([]byte, pad_length)
    p.packBytes([]byte {byte(ln & 255), byte(ln >> 8)}) // little endian int16
    p.packBytes(seg_data)
    p.packBytes(padding)
    p.sendPackets()
}

func (p *wireProtocol)  opCloseBlob(blobHandle int32) {
    p.packInt(op_close_blob)
    p.packInt(blobHandle)
    p.sendPackets()
}

func (p *wireProtocol) opResponse() (int32, int32, []byte, error) {
    b, _ := p.recvPackets(4)
    for {
        if bytes_to_bint32(b) == op_dummy {
            b, _ = p.recvPackets(4)
        }
    }

    if bytes_to_bint32(b) != op_response {
        return 0, 0, nil, errors.New("Error op_response")
    }
    return p._parse_op_response()
}

func (p *wireProtocol) opSqlResponse(xsqlda []xSQLVAR) (*list.List, error){
    b, err := p.recvPackets(4)
    for {
        if bytes_to_bint32(b) == op_dummy {
            b, err = p.recvPackets(4)
        }
    }

    if bytes_to_bint32(b) != op_sql_response {
        return nil, errors.New("Error op_sql_response")
    }

    b, err = p.recvPackets(4)
    // count := int(bytes_to_bint32(b))

    r := list.New()
    var ln int
    for _, x := range xsqlda {
        if x.ioLength() < 0 {
            b, err = p.recvPackets(4)
            ln = int(bytes_to_bint32(b))
        } else {
            ln = x.ioLength()
        }
        raw_value, _ := p.recvPacketsAlignment(ln)
        b, err = p.recvPackets(4)
        if bytes_to_bint32(b) == 0 {    // Not NULL
            r.PushBack(x.value(raw_value))
        } else {
            r.PushBack(nil)
        }
    }

    b, err = p.recvPackets(32)   // ??? 32 bytes skip

    return r, err
}
