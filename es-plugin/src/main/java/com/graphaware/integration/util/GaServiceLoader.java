package com.graphaware.integration.util;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.reflections.Reflections;

public class GaServiceLoader
{
  
  protected final static Logger logger = Logger.getLogger(GaServiceLoader.class.getName());

  private static final Reflections reflections = new Reflections("com.graphaware.integration");

  public static <T, A extends Annotation> HashMap<String, Class<T>> loadClass(Class<T> type, Class<A> annotation)
  {
    return loadClassByAnnotation(type, annotation);
  }

  public static <T, A extends Annotation> HashMap<String, T> load(Class<T> type, Class<A> annotation)
  {
    HashMap<String, T> loader;
    if ((null != (loader = (HashMap<String, T>) loadByAnnotation(type, annotation)))
            && !loader.isEmpty())
    {
      HashMap<String, T> oldLoader = load(type);
      if (oldLoader != null)
        for (String key : oldLoader.keySet())
          if (!loader.containsKey(key))
            loader.put(key, oldLoader.get(key));
      return loader;
    }
    else
      return load(type);
  }

  public static <T> HashMap<String, T> load(Class<T> type)
  {
    HashMap<String, T> loader;
    if (null != (loader = java6Loader(type)))
      return loader;
    return new HashMap<>();
  }

  private static <T> HashMap<String, T> java6Loader(Class<T> type)
  {
    try
    {
      Class<?> serviceLoaderClass = Class.forName("java.util.ServiceLoader");
      Iterable<T> contextClassLoaderServices = (Iterable<T>) serviceLoaderClass.getMethod("load", Class.class).invoke(null, type);
      // Jboss 7 does not export content of META-INF/services to context
      // class loader,
      // so this call adds implementations defined in Neo4j libraries from
      // the same module.
      Iterable<T> currentClassLoaderServices = (Iterable<T>) serviceLoaderClass.
              getMethod("load", Class.class, ClassLoader.class).
              invoke(null, type, GaServiceLoader.class.getClassLoader());
      // Combine services loaded by both context and module classloaders.
      // Service instances compared by full class name ( we cannot use
      // equals for instances or classes because they can came from
      // different classloaders ).
      HashMap<String, T> services = new HashMap<>();
      putAllInstancesToMap(currentClassLoaderServices, services);
      // Services from context class loader have higher precedence
      putAllInstancesToMap(contextClassLoaderServices, services);
      return services;
    }
    catch (Exception e)
    {
      return null;
    }
    catch (LinkageError e)
    {
      return null;
    }
  }

  private static <T, A extends Annotation> HashMap<String, T> loadByAnnotation(Class<T> type, Class<A> annotation)
  {
    HashMap<String, T> loader = new HashMap<>();
    Set<Class<?>> providers = reflections.getTypesAnnotatedWith(annotation);
    try
    {
      for (Class<?> item : providers)
      {
        T newElement = (T) item.newInstance();
        loader.put(newElement.getClass().getName(), newElement);
      }
    }
    catch (InstantiationException | IllegalAccessException ex)
    {
      logger.log(Level.SEVERE, ex.toString(), ex);
    }
    return loader;
  }

  private static <T, A extends Annotation> HashMap<String, Class<T>> loadClassByAnnotation(Class<T> type, Class<A> annotation)
  {
    HashMap<String, Class<T>> loader = new HashMap<>();
    Set<Class<?>> providers = reflections.getTypesAnnotatedWith(annotation);
    for (Class<?> item : providers)
      loader.put(item.getName(), (Class<T>) item);
    return loader;
  }

  private static <T> void putAllInstancesToMap(Iterable<T> services, Map<String, T> servicesMap)
  {
    for (T instance : filterExceptions(services))
    {
      if (null != instance)
      {
        servicesMap.put(instance.getClass().getName(), instance);
      }
    }
  }

  private static <T> Iterable<T> filterExceptions(final Iterable<T> iterable)
  {
    return new Iterable<T>()
    {
      @Override
      public Iterator<T> iterator()
      {
        return new PrefetchingIterator<T>()
        {
          final Iterator<T> iterator = iterable.iterator();

          @Override
          protected T fetchNextOrNull()
          {
            while (iterator.hasNext())
            {
              try
              {
                return iterator.next();
              }
              catch (Throwable e)
              {
              }
            }
            return null;
          }
        };
      }
    };
  }
}
