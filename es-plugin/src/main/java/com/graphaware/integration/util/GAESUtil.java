/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.integration.util;

/**
 *
 * @author ale
 */
public class GAESUtil
{
  public static int getInt(final Object value, final int defaultValue)
  {
    if (value instanceof Number)
    {
      return ((Number) value).intValue();
    }
    else if (value instanceof String)
    {
      return Integer.parseInt(value.toString());
    }
    return defaultValue;
  }
}
