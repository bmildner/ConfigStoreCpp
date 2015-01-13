// Copyright (c) 2014 by Bertolt Mildner
// All rights reserved.

#ifndef CONFIGURATION_RANDOMNUMBERGENERATOR_H
#define CONFIGURATION_RANDOMNUMBERGENERATOR_H

#pragma once

#include <numeric>

#include "Utils.h"

CONFIGURATION_BOOST_INCL_GUARD_BEGIN
#include <boost/random/random_device.hpp>
#include <boost/random/uniform_int_distribution.hpp>
CONFIGURATION_BOOST_INCL_GUARD_END

#include "Configuration.h"

namespace Configuration
{
  namespace Detail
  {
    template <typename Integer>
    class RandomNumberGenerator
    {
      static_assert(std::numeric_limits<Integer>::is_integer, "<Integer> must be an integer type");

      public:
        RandomNumberGenerator()
        : m_RandomDevice(), m_Distribution(std::numeric_limits<Integer>::min(), std::numeric_limits<Integer>::max())
        {
        }

        inline Integer Get()
        {
          return m_Distribution(m_RandomDevice);
        }

      private:
        boost::random::random_device                     m_RandomDevice;
        boost::random::uniform_int_distribution<Integer> m_Distribution;
    };
  }
}

#endif
