package com.algolia.search;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import org.apache.http.conn.DnsResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

class TimeoutableHostNameResolver implements DnsResolver {

  /**
   * Timeout in ms
   */
  private final long timeout;

  private final TimeLimiter timeLimiter;

  TimeoutableHostNameResolver(long timeout) {
    this.timeout = timeout;
    this.timeLimiter = new SimpleTimeLimiter();
  }

  @Override
  public InetAddress[] resolve(final String hostname) throws UnknownHostException {
    try {
      return timeLimiter.callWithTimeout(
//        () -> new InetAddress[]{InetAddress.getByName(hostname)},
              new Callable<InetAddress[]>() {
                @Override
                public InetAddress[] call() throws Exception {
                  return new InetAddress[]{InetAddress.getByName(hostname)};
                }
              },
              timeout,
              TimeUnit.MILLISECONDS,
              true
      );
    } catch (Exception e) {
      throw new UnknownHostException(hostname);
    }
  }
}
