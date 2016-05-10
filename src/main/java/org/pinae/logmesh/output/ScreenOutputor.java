package org.pinae.logmesh.output;

import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.text.SimpleDateFormat;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ScrollPaneConstants;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;

import org.pinae.logmesh.component.ComponentInfo;
import org.pinae.logmesh.message.Message;

/**
 * 消息窗口输出模式
 * 
 * @author Huiyugeng
 * 
 */
public class ScreenOutputor extends ComponentInfo implements MessageOutputor {

	private JFrame frame;
	private JScrollPane scrollPane;
	private JTable table;

	private DefaultTableModel tableModel;

	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	/* 消息计数器 */
	private int messageCounter = 0;
	/* 最大行数 */
	private int maxRows = 50;

	private static boolean INIT_FLAG;

	public ScreenOutputor() {

	}

	public void initialize() {
		if (INIT_FLAG == false) {
			// 初始化GUI元素
			initGUI();
			// 初始化GUI事件
			initEvent();

			// this.maxRows = getIntegerValue("rows", 5);

			INIT_FLAG = true;
		}
	}

	private void initGUI() {
		int width = 900;
		int height = 600;

		this.frame = new JFrame();

		this.frame.setTitle("logmesh");
		this.frame.setBounds(100, 100, width, height);
		this.frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.frame.getContentPane().setLayout(null);

		this.scrollPane = new JScrollPane();
		this.scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_AS_NEEDED);
		this.scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);

		this.tableModel = new DefaultTableModel();
		this.table = new JTable(this.tableModel);

		String[] columnNames = { "#", "Time", "IP", "Message" };
		int[] columnWidths = { 5, 15, 10, 70 };
		for (String columnName : columnNames) {
			this.tableModel.addColumn(columnName);
		}
		TableColumnModel columnModel = this.table.getColumnModel();
		int columnCount = columnModel.getColumnCount();
		for (int i = 0; i < columnCount; i++) {
			TableColumn column = columnModel.getColumn(i);
			column.setPreferredWidth(columnWidths[i] * width);
		}

		this.scrollPane.setViewportView(this.table);

		this.frame.getContentPane().add(this.scrollPane);
		this.frame.setVisible(true);

	}

	private void initEvent() {
		this.frame.addComponentListener(new ComponentAdapter() {
			@Override
			public void componentResized(ComponentEvent event) {
				scrollPane.setBounds(0, 0, frame.getWidth() - 15, frame.getHeight() - 35);
				table.setBounds(scrollPane.getX(), scrollPane.getY(), scrollPane.getWidth(), scrollPane.getHeight());
			}
		});

		this.frame.addWindowListener(new WindowAdapter() {
			@Override
			public void windowOpened(WindowEvent event) {
				frame.setTitle(getStringValue("title", "Logmesh"));

				int width = getIntegerValue("width", 900);
				int height = getIntegerValue("height", 600);
				frame.setBounds(frame.getX(), frame.getY(), width, height);
				scrollPane.setBounds(0, 0, width - 15, height - 35);
				table.setBounds(scrollPane.getX(), scrollPane.getY(), scrollPane.getWidth(), scrollPane.getHeight());
			}

			@Override
			public void windowClosing(WindowEvent event) {
				System.exit(0);
			}
		});

		this.table.addMouseListener(new MouseAdapter() {
			public void mouseClicked(MouseEvent e) {
				if (e.getClickCount() == 2) {

				}
			}
		});
	}

	public void output(Message message) {
		if (message != null) {
			try {
				this.tableModel.addRow(new String[] { Integer.toString(++messageCounter), dateFormat.format(message.getTimestamp()), message.getIP(),
						message.getMessage().toString() });
			} catch (Exception e) {

			}
			int rowCount = this.table.getRowCount();
			if (this.maxRows != 0 && rowCount > this.maxRows) {
				this.tableModel.removeRow(0);
			}
		}
	}

	public void close() {
		if (frame != null) {
			frame.dispose();
		}
	}
}
