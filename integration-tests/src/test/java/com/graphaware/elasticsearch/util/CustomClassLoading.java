/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.graphaware.elasticsearch.util;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author ale
 */
public class CustomClassLoading
{
  private final String libPath;
  private final ClassLoader classloader;
  
  public CustomClassLoading(String libPath, ClassLoader parent) throws MalformedURLException
  {
    this.libPath = libPath;

    List<URL> urls = new ArrayList();
    File directory = new File(libPath);
    if (!directory.exists())
      throw new RuntimeException("Path : " + libPath + " doesn't exist");
    for (File f : directory.listFiles())
    {
      final URL toURL = f.toURI().toURL();
      urls.add(toURL);
      Logger.getLogger(CustomClassLoading.class.getName()).warning(toURL.getPath());
    }
    // feed your URLs to a URLClassLoader!
    this.classloader
            = new URLClassLoader(urls.toArray(new URL[0])
                    ,parent);

    //Thread.currentThread().setContextClassLoader(classloader);
  }
  
  public CustomClassLoading(String libPath) throws MalformedURLException
  {
    this(libPath, ClassLoader.getSystemClassLoader().getParent());    
  }

  public Class<?> loadClass(String classname) throws ClassNotFoundException
  {
    Class loadedClass = classloader.loadClass(classname);
    return loadedClass;
  }
}
