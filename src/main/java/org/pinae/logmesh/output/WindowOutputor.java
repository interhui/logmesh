package org.pinae.logmesh.output;

import java.awt.Color;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.ScrollPaneConstants;
import javax.swing.text.BadLocationException;

import org.pinae.logmesh.message.Message;
import org.pinae.logmesh.processor.ProcessorInfo;

/**
 * 窗口输出模式
 * 
 * @author Huiyugeng
 * 
 */
public class WindowOutputor extends ProcessorInfo implements MessageOutputor {

	private JFrame frame;
	private JTextArea txtConsole;
	private JScrollPane scrollPane;

	private List<Integer> messageLengthList = new ArrayList<Integer>();

	private int maxRows = 100; // 最大行数
	private int bufferRows = maxRows / 10; // 缓冲行数
	private int bufferRowsCount = 0; // 缓冲行数计数器

	public WindowOutputor() {

	}

	public void init() {
		initGUI(); // 初始化GUI元素

		initEvent(); // 初始化GUI事件
		
		this.maxRows = getIntegerValue("rows", 100);
		bufferRows = maxRows / 10;
	}

	public void initGUI() {
		frame = new JFrame();

		frame.setTitle("LogMesh");
		frame.setBounds(100, 100, 695, 500);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);

		scrollPane = new JScrollPane();
		scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
		scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		scrollPane.setBounds(0, 0, 485, 480);

		txtConsole = new JTextArea();
		txtConsole.setLineWrap(true);
		txtConsole.setEditable(false);
		txtConsole.setBackground(Color.BLACK);
		txtConsole.setForeground(new Color(0, 255, 0));
		txtConsole.setBounds(0, 0, 400, 480);
		txtConsole.setColumns(20);
		txtConsole.setRows(100);

		scrollPane.setViewportView(txtConsole);

		frame.getContentPane().add(scrollPane);

		frame.setVisible(true);

	}

	private void initEvent() {
		frame.addComponentListener(new ComponentAdapter() {
			@Override
			public void componentResized(ComponentEvent event) {
				scrollPane.setBounds(0, 0, frame.getWidth() - 15, frame.getHeight() - 35);
				txtConsole.setBounds(scrollPane.getX(), scrollPane.getY(), scrollPane.getWidth(),
						scrollPane.getHeight());
			}
		});

		frame.addWindowListener(new WindowAdapter() {
			@Override
			public void windowOpened(WindowEvent event) {
				frame.setTitle(getStringValue("title", "Logmesh"));

				int width = getIntegerValue("width", 800);
				int height = getIntegerValue("height", 600);
				frame.setBounds(frame.getX(), frame.getY(), width, height);
				scrollPane.setBounds(0, 5, width - 15, height - 35);
				txtConsole.setBounds(scrollPane.getX(), scrollPane.getY(), scrollPane.getWidth(),
						scrollPane.getHeight());

				int columns = getIntegerValue("columns", 50);

				txtConsole.setColumns(columns);
				txtConsole.setRows(maxRows);

				txtConsole.setBackground(Color.decode(getStringValue("background", "#000000")));
				txtConsole.setForeground(Color.decode(getStringValue("foreground", "#00ff00")));
			}

			@Override
			public void windowClosing(WindowEvent event) {
				System.exit(0);
			}
		});
	}

	public void showMessage(Message message) {

		String msg = null;

		if (message != null) {
			msg = message.toString();

			if (msg != null) {

				txtConsole.append(msg);
				messageLengthList.add(msg.length());

				if (messageLengthList.size() > maxRows) {
					if (bufferRowsCount >= bufferRows) {
						int messageLength = 0;
						for (int i = 0; i < bufferRowsCount; i++) {
							messageLength += messageLengthList.get(i);
							messageLengthList.remove(i);
						}
						try {
							txtConsole.getDocument().remove(0, messageLength);
						} catch (BadLocationException e) {
							txtConsole.setText("");
							messageLengthList.clear();
						}
						bufferRowsCount = 0;
					} else {
						bufferRowsCount++;
					}
				}
			}
		}

	}
}
