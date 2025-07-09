import { GenericFormatter, MultiBar, Presets as BarPresets, SingleBar } from "cli-progress";
import { cristal } from "gradient-string";
import pc from "picocolors";

const DEFAULT_KEY = "default";

export class ProgressTracker {
	multibar: MultiBar;
	progress: Record<string, { completed: number; total: number }> = {};
	bars: Record<string, SingleBar> = {};
	speeds: Record<string, number[]> = {};

	constructor(private keys: string[] = [DEFAULT_KEY]) {
		this.multibar = new MultiBar({
			format: this.formatter,
			forceRedraw: true,
			clearOnComplete: true,
		});
	}

	incrementCompleted(key: string | undefined = DEFAULT_KEY) {
		if (key) this.setCompleted(key, this.progress[key]?.completed + 1 || 0);
	}

	setCompleted(key: string | undefined, value: number): void;
	setCompleted(value: number): void;
	setCompleted(keyOrValue: string | undefined | number, value?: number) {
		if (typeof keyOrValue === "number") {
			this.setCompleted(DEFAULT_KEY, keyOrValue);
		} else {
			if (!keyOrValue || !this.progress[keyOrValue] || !this.bars[keyOrValue]) return;
			this.progress[keyOrValue].completed = value!;
			this.bars[keyOrValue].update(this.progress[keyOrValue].completed);
		}
	}

	incrementTotal(key: string | undefined = DEFAULT_KEY) {
		if (key) this.setTotal(key, this.progress[key]?.total + 1 || 0);
	}

	setTotal(key: string | undefined, value: number): void;
	setTotal(value: number): void;
	setTotal(keyOrValue: string | undefined | number, value?: number) {
		if (typeof keyOrValue === "number") {
			this.setTotal(DEFAULT_KEY, keyOrValue);
		} else {
			if (!keyOrValue || !this.progress[keyOrValue] || !this.bars[keyOrValue]) return;
			this.progress[keyOrValue].total = value!;
			this.bars[keyOrValue].setTotal(this.progress[keyOrValue].total);
		}
	}

	incrementWhenDone(key: string | undefined = DEFAULT_KEY) {
		if (!key || !this.progress[key] || !this.bars[key]) return;
		this.incrementTotal(key);
		return { [Symbol.dispose]: () => this.incrementCompleted(key) };
	}

	start() {
		for (const key of this.keys) {
			const progress = { completed: 0, total: 0 };
			this.progress[key] = progress;
			this.bars[key] = this.multibar.create(0, 0, { key }, { clearOnComplete: false });

			const speeds = this.speeds;
			speeds[key] = [];
			let prevCompleted = 0;
			setTimeout(function updateSpeed() {
				const currentSpeed = progress.completed - prevCompleted;
				speeds[key].push(currentSpeed);
				if (speeds[key].length > 5) {
					speeds[key].shift();
				}
				prevCompleted = progress.completed;
				setTimeout(updateSpeed, 1000);
			}, 1000);
		}

		const { multibar } = this;
		const consoleLog = console.log, consoleWarn = console.warn, consoleError = console.error;
		console.log = (...data: string[]) => multibar.log(data.join(" ") + "\n");
		console.warn = (...data: string[]) => console.log(pc.yellow(data.join(" ")));
		console.error = (...data: string[]) => console.log(pc.red(data.join(" ")));

		return {
			[Symbol.dispose]() {
				multibar.stop();
				console.log = consoleLog;
				console.warn = consoleWarn;
				console.error = consoleError;
			},
		};
	}

	private formatter: GenericFormatter = (options, params, payload: { key: string }) => {
		const barSize = options.barsize ?? 40;
		const completeSize = Math.max(
			0,
			Math.min(Math.round(barSize * params.value / params.total), barSize),
		);
		const remainingSize = Math.max(0, Math.min(barSize - completeSize, barSize));

		const c = BarPresets.shades_classic.barCompleteChar;
		const r = BarPresets.shades_classic.barIncompleteChar;

		let barUncolored = `${c.repeat(completeSize)}${r.repeat(remainingSize)}`;
		if (!barUncolored.length) barUncolored = r.repeat(barSize);
		const bar = cristal(barUncolored);

		const speedArray = this.speeds[payload.key] ?? [];
		const averageSpeed = speedArray.length > 0
			? speedArray.reduce((sum, speed) => sum + speed, 0) / speedArray.length
			: 0;
		const speed = Math.round(averageSpeed).toString().padStart(2, "0");

		let str = payload.key === DEFAULT_KEY ? "" : `${payload.key} `;
		str += `${bar} ${params.value}/${params.total}`;
		if (speedArray.length > 0) {
			str += ` - ${speed} per sec`;
		}

		return str;
	};
}
