/*
 * Copyright (C) 2019-2023 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * Poseidon is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Poseidon is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Poseidon. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef graph_exceptions_hpp_
#define graph_exceptions_hpp_

#include <exception>
#include <string>

class unknown_id : public std::exception {
  const char *what() const noexcept override {
    return "Node or relationship does not exist.";
  }
};

class unknown_index : public std::exception {
  const char *what() const noexcept override {
    return "Index does not exist.";
  }
};

class index_out_of_range : public std::exception {
  const char *what() const noexcept override {
    return "Index out of range in vector.";
  }
};

class unknown_property : public std::exception {
  const char *what() const noexcept override {
    return "Property does not exist.";
  }
};

class unknown_db : public std::exception {
  const char *what() const noexcept override {
    return "Graph database does not exist.";
  }
};
class invalid_typecast : public std::exception {
  const char *what() const noexcept override {
    return "Invalid typecast for property value.";
  }
};

class transaction_abort : public std::exception {
  const char *what() const noexcept override { return "Transaction aborted."; }
};

class out_of_transaction_scope : public std::exception {
  const char *what() const noexcept override {
    return "Out of transaction scope.";
  }
};

class invalid_nested_transaction : public std::exception {
  const char *what() const noexcept override {
    return "Nested transactions are not supported.";
  }
};

class orphaned_relationship : public std::exception {
  const char *what() const noexcept override {
    return "Cannot delete node(s) since it has relationship.";
  }
};

class udf_not_found : public std::exception {
  const char *what() const noexcept override {
    return "Cannot find UDF library.";
  }
};

class file_not_found : public std::exception {
public:
  file_not_found() : msg_("Cannot open file.") {}
  file_not_found(const std::string& fname) : msg_("Cannot open file '" + fname + "'.") {}
  const char *what() const noexcept override {
    return msg_.c_str();
  }

private:
  std::string msg_;
};

class query_processing_error : public std::exception {
public:
  query_processing_error() : msg_("Unknown query processing error.") {}
  query_processing_error(const std::string& msg) : msg_(msg) {}
  const char *what() const noexcept override {
    return msg_.c_str();
  }

private:
  std::string msg_;
};

class bufferpool_overrun : public std::exception {
  const char *what() const noexcept override { return "Bufferpool overrun."; }
};

class invalid_csr_update : public std::exception {
  const char *what() const noexcept override {
    return "Cannot update CSR to an older snapshot.";
  }
};

#endif