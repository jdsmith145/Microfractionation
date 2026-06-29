#!/usr/bin/env python3
"""Shared anchored help popovers for the CustomTkinter workflow GUIs."""
from __future__ import annotations

import sys
from typing import Any


def _windows_monitor_work_area(x: int, y: int) -> tuple[int, int, int, int] | None:
    """Return the Windows work area containing a screen coordinate."""

    if not sys.platform.startswith("win"):
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class MonitorInfo(ctypes.Structure):
            _fields_ = [
                ("cbSize", wintypes.DWORD),
                ("rcMonitor", wintypes.RECT),
                ("rcWork", wintypes.RECT),
                ("dwFlags", wintypes.DWORD),
            ]

        point = wintypes.POINT(int(x), int(y))
        monitor = ctypes.windll.user32.MonitorFromPoint(point, 2)
        if not monitor:
            return None
        info = MonitorInfo()
        info.cbSize = ctypes.sizeof(info)
        if not ctypes.windll.user32.GetMonitorInfoW(monitor, ctypes.byref(info)):
            return None
        rect = info.rcWork
        return int(rect.left), int(rect.top), int(rect.right), int(rect.bottom)
    except Exception:
        return None


class HelpPopoverController:
    """Create compact, in-window popovers anchored to ``?`` help bubbles."""

    def __init__(
        self,
        root: Any,
        ctk: Any,
        colors: dict[str, str],
        font: Any,
        *,
        target_width: int = 340,
        show_delay_ms: int = 220,
        hide_delay_ms: int = 130,
    ) -> None:
        self.root = root
        self.ctk = ctk
        self.colors = {str(key).lower(): value for key, value in colors.items()}
        self.font = font
        self.target_width = target_width
        self.show_delay_ms = show_delay_ms
        self.hide_delay_ms = hide_delay_ms
        self.panel: Any = None
        self._content: Any = None
        self.trigger: Any = None
        self.text = ""
        self.pinned = False
        self._show_after: str | None = None
        self._hide_after: str | None = None
        self._track_after: str | None = None
        self._layout_signature: tuple[int, int, float] | None = None
        root.bind_all("<Button-1>", self._on_global_click, add="+")
        root.bind_all("<Escape>", self._on_escape, add="+")

    def create_bubble(self, parent: Any, text: str) -> Any:
        bubble = self.ctk.CTkLabel(
            parent,
            text="?",
            width=23,
            height=23,
            corner_radius=12,
            fg_color=self.colors.get("card_alt", "#2d333c"),
            text_color=self.colors.get("muted", "#aab4c0"),
            font=("Segoe UI", 11, "bold"),
            cursor="hand2",
        )
        bubble.bind("<Enter>", lambda _event, widget=bubble, hint=text: self._schedule_show(widget, hint), add="+")
        bubble.bind("<Leave>", self._schedule_hide, add="+")
        bubble.bind("<Button-1>", lambda _event, widget=bubble, hint=text: self._toggle_pin(widget, hint), add="+")
        return bubble

    def _cancel(self, after_id: str | None) -> None:
        if after_id:
            try:
                self.root.after_cancel(after_id)
            except Exception:
                pass

    def _schedule_show(self, trigger: Any, text: str) -> None:
        self._cancel(self._hide_after)
        self._hide_after = None
        if self.panel is not None and self.trigger is trigger:
            return
        if self.pinned and self.trigger is not trigger:
            self.close()
        self._cancel(self._show_after)
        self._show_after = self.root.after(self.show_delay_ms, lambda: self.show(trigger, text, pinned=False))

    def _schedule_hide(self, _event: Any = None) -> None:
        self._cancel(self._show_after)
        self._show_after = None
        if not self.pinned:
            self._cancel(self._hide_after)
            self._hide_after = self.root.after(self.hide_delay_ms, self._hide_if_outside)

    def _toggle_pin(self, trigger: Any, text: str) -> None:
        self._cancel(self._show_after)
        self._show_after = None
        if self.panel is not None and self.trigger is trigger and self.pinned:
            self.close()
            return
        self.show(trigger, text, pinned=True)

    def _on_escape(self, _event: Any = None) -> None:
        self.close()

    def _on_global_click(self, event: Any) -> None:
        if not self.pinned or self.panel is None:
            return
        if self._is_descendant(event.widget, self.panel) or self._is_descendant(event.widget, self.trigger):
            return
        self.close()

    @staticmethod
    def _is_descendant(widget: Any, ancestor: Any) -> bool:
        while widget is not None:
            if widget is ancestor:
                return True
            widget = getattr(widget, "master", None)
        return False

    @staticmethod
    def _pointer_inside(widget: Any) -> bool:
        try:
            x = widget.winfo_pointerx()
            y = widget.winfo_pointery()
            return (
                widget.winfo_rootx() <= x <= widget.winfo_rootx() + widget.winfo_width()
                and widget.winfo_rooty() <= y <= widget.winfo_rooty() + widget.winfo_height()
            )
        except Exception:
            return False

    def _hide_if_outside(self) -> None:
        self._hide_after = None
        if self.pinned:
            return
        if self.trigger is not None and self._pointer_inside(self.trigger):
            return
        if self.panel is not None and self._pointer_inside(self.panel):
            return
        self.close()

    def _visible_bounds(self, trigger: Any, *, margin: int = 12) -> tuple[int, int, int, int]:
        self.root.update_idletasks()
        root_x = int(self.root.winfo_rootx())
        root_y = int(self.root.winfo_rooty())
        root_right = root_x + int(self.root.winfo_width())
        root_bottom = root_y + int(self.root.winfo_height())
        anchor_x = int(trigger.winfo_rootx() + trigger.winfo_width() / 2)
        anchor_y = int(trigger.winfo_rooty() + trigger.winfo_height() / 2)
        work = _windows_monitor_work_area(anchor_x, anchor_y)
        if work is None:
            work = (0, 0, int(self.root.winfo_screenwidth()), int(self.root.winfo_screenheight()))
        screen_left, screen_top, screen_right, screen_bottom = work
        left = max(root_x, screen_left) - root_x + margin
        top = max(root_y, screen_top) - root_y + margin
        right = min(root_right, screen_right) - root_x - margin
        bottom = min(root_bottom, screen_bottom) - root_y - margin
        if right <= left or bottom <= top:
            return 0, 0, max(1, root_right - root_x), max(1, root_bottom - root_y)
        return int(left), int(top), int(right), int(bottom)

    @staticmethod
    def _scale_for(widget: Any) -> float:
        try:
            return float(widget._apply_widget_scaling(1.0))
        except Exception:
            return 1.0

    @staticmethod
    def _to_widget_units(widget: Any, pixels: int | float) -> float:
        try:
            return float(widget._reverse_widget_scaling(pixels))
        except Exception:
            return float(pixels)

    @staticmethod
    def _place_in_pixels(widget: Any, x: int, y: int) -> None:
        """Place using Tk rendered pixels, bypassing CTk's second coordinate scaling."""

        widget.tk.call("place", "configure", widget._w, "-x", int(x), "-y", int(y))

    def _trigger_visible(self) -> bool:
        if self.trigger is None:
            return False
        try:
            if not self.trigger.winfo_exists() or not self.trigger.winfo_ismapped():
                return False
            root_x = int(self.root.winfo_rootx())
            root_y = int(self.root.winfo_rooty())
            bounds = self._visible_bounds(self.trigger, margin=0)
            x = int(self.trigger.winfo_rootx() - root_x)
            y = int(self.trigger.winfo_rooty() - root_y)
            width = int(self.trigger.winfo_width())
            height = int(self.trigger.winfo_height())
            if x + width <= bounds[0] or x >= bounds[2] or y + height <= bounds[1] or y >= bounds[3]:
                return False
            center_x = int(self.trigger.winfo_rootx() + width / 2)
            center_y = int(self.trigger.winfo_rooty() + height / 2)
            visible_widget = self.root.winfo_containing(center_x, center_y)
            return visible_widget is not None and self._is_descendant(visible_widget, self.trigger)
        except Exception:
            return False

    @staticmethod
    def _overflow(candidate: tuple[int, int], width: int, height: int, bounds: tuple[int, int, int, int]) -> int:
        x, y = candidate
        left, top, right, bottom = bounds
        return max(0, left - x) + max(0, top - y) + max(0, x + width - right) + max(0, y + height - bottom)

    def _position(self, trigger: Any, width: int, height: int, bounds: tuple[int, int, int, int]) -> tuple[int, int]:
        left, top, right, bottom = bounds
        gap = 8
        anchor_left = int(trigger.winfo_rootx() - self.root.winfo_rootx())
        anchor_top = int(trigger.winfo_rooty() - self.root.winfo_rooty())
        anchor_right = anchor_left + int(trigger.winfo_width())
        anchor_bottom = anchor_top + int(trigger.winfo_height())
        anchor_mid_x = int((anchor_left + anchor_right) / 2)
        anchor_mid_y = int((anchor_top + anchor_bottom) / 2)
        candidates = [
            (anchor_right + gap, anchor_mid_y - height // 2),
            (anchor_left - width - gap, anchor_mid_y - height // 2),
            (anchor_mid_x - width // 2, anchor_bottom + gap),
            (anchor_mid_x - width // 2, anchor_top - height - gap),
        ]
        for x, y in candidates:
            if left <= x and top <= y and x + width <= right and y + height <= bottom:
                return x, y
        x, y = min(candidates, key=lambda candidate: self._overflow(candidate, width, height, bounds))
        return max(left, min(x, right - width)), max(top, min(y, bottom - height))

    def _configure_content(self, bounds: tuple[int, int, int, int]) -> tuple[int, int]:
        if self.panel is None:
            return 0, 0
        if self._content is not None:
            self._content.destroy()
            self._content = None
        available_width = max(1, bounds[2] - bounds[0])
        available_height = max(1, bounds[3] - bounds[1])
        width = min(self.target_width, available_width)
        max_panel_height = min(available_height, 360)
        wrap_width = max(40, width - 30)
        self.panel.configure(
            width=self._to_widget_units(self.panel, width),
            height=self._to_widget_units(self.panel, 54),
        )
        label = self.ctk.CTkLabel(
            self.panel,
            text=self.text,
            font=self.font,
            text_color=self.colors.get("text", "#f3f6fa"),
            justify="left",
            wraplength=self._to_widget_units(self.panel, wrap_width),
            padx=14,
            pady=11,
            anchor="w",
        )
        label.pack(fill="both", expand=True)
        self._content = label
        self._place_in_pixels(self.panel, -10000, -10000)
        self.panel.update_idletasks()
        requested_height = max(54, int(label.winfo_reqheight()) + 12)
        height = min(requested_height, max_panel_height)
        if requested_height > max_panel_height:
            label.destroy()
            text_box = self.ctk.CTkTextbox(
                self.panel,
                width=self._to_widget_units(self.panel, max(1, width - 8)),
                height=self._to_widget_units(self.panel, max(1, height - 8)),
                corner_radius=8,
                border_width=0,
                border_spacing=9,
                fg_color="transparent",
                text_color=self.colors.get("text", "#f3f6fa"),
                scrollbar_button_color=self.colors.get("card_alt", "#2d333c"),
                scrollbar_button_hover_color=self.colors.get("border", "#3d4652"),
                font=self.font,
                wrap="word",
            )
            text_box.pack(fill="both", expand=True, padx=4, pady=4)
            text_box.insert("1.0", self.text)
            text_box.configure(state="disabled")
            self._content = text_box
        self.panel.configure(
            width=self._to_widget_units(self.panel, width),
            height=self._to_widget_units(self.panel, height),
        )
        self.panel.update_idletasks()
        return int(self.panel.winfo_width()), int(self.panel.winfo_height())

    def _layout_panel(self, *, force_content: bool = False) -> None:
        if self.panel is None or self.trigger is None:
            return
        bounds = self._visible_bounds(self.trigger)
        signature = (
            int(bounds[2] - bounds[0]),
            int(bounds[3] - bounds[1]),
            round(self._scale_for(self.panel), 4),
        )
        if force_content or self._layout_signature != signature:
            width, height = self._configure_content(bounds)
            self._layout_signature = signature
        else:
            width = int(self.panel.winfo_width())
            height = int(self.panel.winfo_height())
        x, y = self._position(self.trigger, width, height, bounds)
        self._place_in_pixels(self.panel, x, y)
        self.panel.lift()

    def _track_panel(self) -> None:
        self._track_after = None
        if self.panel is None:
            return
        if not self._trigger_visible():
            self.close()
            return
        self._layout_panel()
        self._track_after = self.root.after(100, self._track_panel)

    def show(self, trigger: Any, text: str, *, pinned: bool) -> None:
        self._cancel(self._show_after)
        self._cancel(self._hide_after)
        self._cancel(self._track_after)
        self._show_after = None
        self._hide_after = None
        self._track_after = None
        if self.panel is not None:
            self.panel.destroy()
        self._content = None
        self.trigger = trigger
        self.text = text
        self.pinned = pinned
        self.panel = self.ctk.CTkFrame(
            self.root,
            fg_color="#111318",
            border_color=self.colors.get("border", "#3d4652"),
            border_width=1,
            corner_radius=12,
            width=1,
            height=1,
        )
        self.panel.pack_propagate(False)
        self._layout_signature = None
        self._layout_panel(force_content=True)
        self.panel.bind("<Enter>", lambda _event: self._cancel(self._hide_after), add="+")
        self.panel.bind("<Leave>", self._schedule_hide, add="+")
        self._track_after = self.root.after(100, self._track_panel)

    def close(self) -> None:
        self._cancel(self._show_after)
        self._cancel(self._hide_after)
        self._cancel(self._track_after)
        self._show_after = None
        self._hide_after = None
        self._track_after = None
        self._layout_signature = None
        self.pinned = False
        self.trigger = None
        if self.panel is not None:
            try:
                self.panel.destroy()
            except Exception:
                pass
            self.panel = None
        self._content = None
