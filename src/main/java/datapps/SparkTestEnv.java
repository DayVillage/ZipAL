package datapps;

import java.io.File;

public class SparkTestEnv {

	public static String SPARK_MASTER = "local[4]";
	public static Boolean SERIALIZATION_DEBUG_FLAG = true;
	public static String HADOOP_HOME = "lib/hadoop";

	public static void init() {
		try {
			// set Spark Master to local
			if (!System.getProperties().containsKey("spark.master")) {
				System.getProperties().setProperty("spark.master", SPARK_MASTER);
			}

			// serialization debug flag
			System.getProperties().setProperty("sun.io.serialization.extendedDebugInfo", SERIALIZATION_DEBUG_FLAG.toString());

			// HDFS win utils
			if (!System.getProperties().containsKey("hadoop.home.dir")) {
				File workaround = new File(HADOOP_HOME);
				System.getProperties().setProperty("hadoop.home.dir", workaround.getAbsolutePath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean deleteFolder(String folderName) {
		System.out.println("try to delete folder: " + new File(folderName).getAbsolutePath());
		return deleteFolder(new File(folderName), true);
	}

	public static boolean deleteFolder(File folder, boolean force) {
		boolean result = false;
		if (folder.exists()) {
			File[] files = folder.listFiles();
			if (files != null) {
				for (File file : files) {
					if (!force) {
						throw new RuntimeException("The folder is not empty");
					}
					if (file.isDirectory()) {
						result = deleteFolder(file, force);
						if (!result) {
							System.err.println("delete " + file + " failed.");
						}
					} else {
						// System.out.println("delete " + file);
						result = file.delete();
						if (!result) {
							System.err.println("delete " + file + " failed.");
						}
					}
				}
			}
			// System.out.println("delete " + folder);
			result = folder.delete();
		}
		return result;
	}

	public static void main(String[] args) {
		SparkTestEnv.deleteFolder("target/generated-sources");
	}
}
