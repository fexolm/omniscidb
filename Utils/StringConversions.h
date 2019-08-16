/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _STRING_CONVERSIONS_H_
#define _STRING_CONVERSIONS_H_
#include <boost/lexical_cast.hpp>
#include <boost/utility/string_view.hpp>

namespace StringConversions {

const inline std::string& to_string(const std::string& s) {
  return s;
}

std::string inline to_string(const boost::string_view& s) {
  return s.to_string();
}

template <typename String>
int64_t strtol(const String &s) {
    int64_t res = 0;
    for(auto i = s.begin(); i != s.end(); i++) {
        res = res * 10 + (*i - '0');
    }
    return res;
}

template <typename String>
double strtod(const String &s) {
    return boost::lexical_cast<double>(s.data(), s.size());
}

template <typename String>
float strtof(const String &s) {
    return boost::lexical_cast<float>(s.data(), s.size());
}
}  // namespace StringConversions
#endif /* _STRING_CONVERSIONS_H_ */