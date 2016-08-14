package fgh.util;

/**
 * 
 * @author fgh
 * @since 2016年8月14日下午4:11:40
 */
public class Utils {

	public static void waitForSeconds(int seconds) {
		try {
			Thread.sleep(seconds*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void waitForMillis(long milliSeconds) {
		try {
			Thread.sleep(milliSeconds);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
