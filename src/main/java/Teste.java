
public class Teste {
	
	public static void main(String[] args) {
		int headerIn = 2;
		int posIn = 99999999;

		int c = headerIn | (posIn << 16);

		int headerOut = c & 0x0000FFFF;
		int posOut = c >> 16;

		System.out.println("headerIn: " + headerIn);
		System.out.println("posIn: " + posIn);
		System.out.println("code: " + c);
		System.out.println("headerOut: " + headerOut);
		System.out.println("codeOut: " + posOut);
	}

}
