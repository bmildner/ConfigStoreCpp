// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#ifndef CONFIGURATION_UTILS_H
#define CONFIGURATION_UTILS_H

#pragma once

#include <cstdint>
#include <string>
#include <type_traits>
#include <limits>

#ifndef noexcept
/// Compatibility with non-clang compilers.
# ifndef __has_feature
#  define __has_feature(x) 0
# endif
// Detect whether the compiler supports C++11 noexcept exception specifications.
# if (defined(__GNUC__) && (__GNUC__ >= 4 && __GNUC_MINOR__ >= 7 ) && defined(__GXX_EXPERIMENTAL_CXX0X__))
// GCC 4.7 and following have noexcept
# elif defined(__clang__) && __has_feature(cxx_noexcept)
// Clang 3.0 and above have noexcept
# elif defined(_MSC_VER) && (_MSC_VER >= 1900)
// MSVC 2015 and above should have noexcept!?
# else
#  define noexcept throw()
# endif
#endif

// "inclusion guard" macros for boost headers, dreaded MSVC code analysis causes warnings in boost headers ...
# ifdef _MSC_VER
#  include <codeanalysis\warnings.h>

#  define CONFIGURATION_BOOST_INCL_GUARD_BEGIN  __pragma(warning(push))                                 \
                                                __pragma(warning(disable: ALL_CODE_ANALYSIS_WARNINGS))

#  define CONFIGURATION_BOOST_INCL_GUARD_END    __pragma(warning(pop))

# else
#  define CONFIGURATION_BOOST_INCL_GUARD_BEGIN
#  define CONFIGURATION_BOOST_INCL_GUARD_END
# endif


namespace Configuration
{
  struct Exception;

  // TODO: add support for nested exceptions! [? default template param + specialization of a helper we additionally derive from? :]

  template <typename Interface>
  class ExceptionImpl final: public Interface, public virtual std::exception
  {
    public:
      explicit ExceptionImpl(const std::wstring& what)
      : Interface(),
        m_What(what), m_NarrowWhat(WideToNarrowStr(m_What)),
        m_TypeName(NarrowToWideStr(typeid(Interface).name()))
      {
      }

      virtual const std::wstring& What() const noexcept override
      {
        return m_What;
      }

      virtual const char* what() const noexcept override
      {
        return m_NarrowWhat.c_str();
      }


      virtual const std::wstring& TypeName() const noexcept override
      {
        return m_TypeName;
      }

    private:
      static_assert(std::is_base_of<Configuration::Exception, Interface>::value, "<Interface> needs to be derived from Configuration::Exception!");

      std::wstring m_What;
      std::string  m_NarrowWhat;
      std::wstring m_TypeName;
  };

  std::string WcharToUTF8(const std::wstring& str);
  std::wstring UTF8ToWchar(const std::string& str);

  std::string WideToNarrowStr(const std::wstring& wide, char replacementChar = '?');
  std::wstring NarrowToWideStr(const std::string& narrow);

  using ProcessToken = std::uint64_t;

  ProcessToken GetProcessToken();

  static_assert(std::is_same<ProcessToken, decltype(GetProcessToken())>::value, "GetProcessToken() must return a ProcessToken");
  static_assert((sizeof(GetProcessToken()) * 8) >= 64, "GetProcessToken() needs to return at least 64 bits");

}

#endif
