USAGE:  hadoop jar urlspliter-1.0-SNAPSHOT.jar /DIR/TO/SRC_URL/ /DIR/FOR/STORAGE/ FILE_PREFIX_NAME FILE_SIZE_ALLOWED_KB       

- /DIR/TO/SRC_URL/ : url的源目录       
- /DIR/FOR/STORAGE/ : 切割后url的存放目录. 运行后会在此目录下生成对应域名的文件夹(如 sina.com.cn )      
- FILE_PREFIX_NAME : 切割后保存url的文件名的前缀      
- FILE_SIZE_ALLOWED_KB : 切割文件的最大容量(注意这里不是严格的: 只在切割之前判断, 过程中不会再判断), 单位是KB        
