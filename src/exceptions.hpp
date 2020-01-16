/*
 * Copyright (C) 2019-2020 DBIS Group - TU Ilmenau, All Rights Reserved.
 *
 * This file is part of the Poseidon package.
 *
 * PipeFabric is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PipeFabric is distributed in the hope that it will be useful,
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

class unknown_id : public std::exception {
  const char *what() const noexcept override {
    return "Node or relationship does not exist.";
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

#endif